package signal.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.setOnInsert;
import static java.lang.System.identityHashCode;
import static java.lang.System.nanoTime;
import static signal.mongo.CollectionNamed.BARRIER_NAMED;
import static signal.mongo.MongoErrorCode.LOCK_BUSY;
import static signal.mongo.MongoErrorCode.LOCK_FAILED;
import static signal.mongo.MongoErrorCode.LOCK_TIMEOUT;
import static signal.mongo.MongoErrorCode.NO_SUCH_TRANSACTION;
import static signal.mongo.MongoErrorCode.WRITE_CONFLICT;
import static signal.mongo.Utils.parkCurrentThreadUntil;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.errorprone.annotations.DoNotCall;
import com.google.errorprone.annotations.Keep;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import signal.api.DistributeBarrier;
import signal.api.Lease;
import signal.api.SignalException;
import signal.mongo.pojo.BarrierDocument;
import signal.mongo.pojo.BarrierWaiterDocument;

/**
 * 数据存储格式
 *
 * <pre>{@code
 *  {
 *   _id: 'Test-Barrier',
 *   v: Long('7'),
 *   w: [
 *     {
 *       lease: '29052205361338000'
 *     },
 *     {
 *       lease: '29052205361338000'
 *     }
 *   ]
 * }
 * }</pre>
 */
@ThreadSafe
@AutoService(DistributeBarrier.class)
public final class DistributeBarrierImp extends DistributeMongoSignalBase<BarrierDocument>
    implements DistributeBarrier {

  private final ReentrantLock lock;
  private final Condition removed;
  private final EventBus eventBus;

  @Keep
  @GuardedBy("varHandle")
  @VisibleForTesting
  StatefulVar<Boolean> statefulVar;

  private final VarHandle varHandle;

  public DistributeBarrierImp(
      Lease lease, String key, MongoClient mongoClient, MongoDatabase db, EventBus eventBus) {
    super(lease, key, mongoClient, db, BARRIER_NAMED);
    this.lock = new ReentrantLock();
    this.removed = lock.newCondition();
    this.eventBus = eventBus;

    try {
      this.varHandle =
          MethodHandles.lookup()
              .findVarHandle(DistributeBarrierImp.class, "statefulVar", StatefulVar.class);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException();
    }

    BiFunction<ClientSession, MongoCollection<BarrierDocument>, Boolean> command =
        (session, coll) ->
            coll.findOneAndUpdate(
                    session,
                    eq("_id", this.getKey()),
                    combine(
                        setOnInsert("_id", this.getKey()),
                        setOnInsert("version", 1L),
                        setOnInsert("waiters", Collections.emptyList())),
                    UPSERT_OPTIONS)
                != null;

    for (; ; ) {
      if (commandExecutor.loopExecute(
          command,
          commandExecutor.defaultDBErrorHandlePolicy(WRITE_CONFLICT, NO_SUCH_TRANSACTION),
          null,
          t -> !t)) break;
    }
    this.statefulVar = new StatefulVar<>(true);
    this.eventBus.register(this);
  }

  @Override
  public void await(Long waitTime, TimeUnit timeUnit) throws InterruptedException {
    checkState();
    BarrierWaiterDocument waiter = buildCurrentWaiter();

    BiFunction<ClientSession, MongoCollection<BarrierDocument>, Boolean> command =
        (session, coll) -> {
          StatefulVar<Boolean> currState = (StatefulVar<Boolean>) varHandle.getAcquire(this);
          int currStateHashCode = identityHashCode(currState);

          var filter = eq("_id", getKey());
          BarrierDocument distributeBarrier;

          // 返回成功，但是不阻塞当前线程
          if ((distributeBarrier = coll.find(session, filter).first()) == null) {
            if (identityHashCode(
                    varHandle.compareAndExchangeRelease(this, currState, new StatefulVar<>(true)))
                == currStateHashCode) {
              return true;
            }
            return false;
          }

          long version = distributeBarrier.version(), newVersion = version + 1;
          var newFilter = and(filter, eq("version", version));
          var update = combine(addToSet("waiters", waiter), inc("version", newVersion));

          // 如果barrier为空，需要重试继续更新
          if ((distributeBarrier =
                  coll.findOneAndUpdate(session, newFilter, update, UPDATE_OPTIONS))
              == null) return false;

          List<BarrierWaiterDocument> waiters = distributeBarrier.waiters();
          return waiters != null && waiters.contains(waiter);
        };

    for (; ; ) {
      if (commandExecutor.loopExecute(
          command,
          commandExecutor.defaultDBErrorHandlePolicy(WRITE_CONFLICT, NO_SUCH_TRANSACTION),
          null,
          t -> !t)) break;
    }

    long s = nanoTime(), waitTimeNanos = timeUnit.toNanos(waitTime);
    boolean timed = waitTime > 0;

    boolean elapsed =
        parkCurrentThreadUntil(
            lock,
            removed,
            () -> ((StatefulVar<Boolean>) varHandle.getAcquire(this)).value,
            timed,
            s,
            (waitTimeNanos - (nanoTime() - s)));
    if (!elapsed) throw new SignalException("Timeout.");
  }

  private BarrierWaiterDocument buildCurrentWaiter() {
    return new BarrierWaiterDocument(
        Utils.getCurrentHostname(), this.getLease().getId(), Utils.getCurrentThreadName());
  }

  @Override
  public void removeBarrier() {
    checkState();
    BiFunction<ClientSession, MongoCollection<BarrierDocument>, Boolean> command =
        (session, coll) -> {
          var filter = eq("_id", getKey());

          // 如果要移除的Barrier不存在，则不向上抛出错误
          BarrierDocument barrier;
          if ((barrier = coll.find(session, filter).first()) == null) return true;
          var newFilter = and(filter, eq("version", barrier.version()));
          DeleteResult deleteResult = coll.deleteOne(session, newFilter);
          return deleteResult.getDeletedCount() == 1L;
        };

    for (; ; ) {
      if (commandExecutor.loopExecute(
          command,
          commandExecutor.defaultDBErrorHandlePolicy(WRITE_CONFLICT, NO_SUCH_TRANSACTION),
          null,
          t -> !t)) break;
    }
  }

  @Override
  public int getWaiterCount() {
    BiFunction<ClientSession, MongoCollection<BarrierDocument>, Integer> command =
        (session, coll) -> {
          var filter = eq("_id", getKey());
          BarrierDocument document = coll.find(filter).limit(1).first();
          return document == null ? 0 : document.waiters().size();
        };
    return commandExecutor.loopExecute(
        command,
        commandExecutor.defaultDBErrorHandlePolicy(
                LOCK_BUSY, LOCK_FAILED, LOCK_TIMEOUT, NO_SUCH_TRANSACTION),
        null,
        t -> false);
  }

  @Override
  public Collection<?> getParticipants() {
    BiFunction<ClientSession, MongoCollection<BarrierDocument>, List<BarrierWaiterDocument>>
        command =
            (session, coll) -> {
              var filter = eq("_id", getKey());
              BarrierDocument document = coll.find(filter).limit(1).first();
              return document == null ? Collections.emptyList() : document.waiters();
            };
    return commandExecutor.loopExecute(
        command,
        commandExecutor.defaultDBErrorHandlePolicy(
                LOCK_BUSY, LOCK_FAILED, LOCK_TIMEOUT, NO_SUCH_TRANSACTION),
        null,
        t -> false);
  }

  @DoNotCall
  @Subscribe
  void awakeAll(ChangeStreamEvents.BarrierRemovedEvent event) {
    if (!this.getKey().equals(event.barrierKey())) return;
    Next:
    for (; ; ) {
      StatefulVar<Boolean> currState = (StatefulVar<Boolean>) varHandle.getAcquire(this);
      int currStateHashCode = identityHashCode(currState);

      if (identityHashCode(
              varHandle.compareAndExchangeRelease(this, currState, new StatefulVar<>(false)))
          == currStateHashCode) {
        lock.lock();
        try {
          removed.signalAll();
        } finally {
          lock.unlock();
        }
        break Next;
      }
      Thread.onSpinWait();
      continue Next;
    }
  }

  @Override
  protected void doClose() {
    eventBus.unregister(this);
    lock.lock();
    try {
      removed.signalAll();
    } finally {
      lock.unlock();
    }
  }
}
