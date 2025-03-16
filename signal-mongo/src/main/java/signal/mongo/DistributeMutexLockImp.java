package signal.mongo;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.size;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static java.lang.System.identityHashCode;
import static java.lang.System.nanoTime;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static signal.mongo.CollectionNamed.MUTEX_LOCK_NAMED;
import static signal.mongo.MongoErrorCode.DuplicateKey;
import static signal.mongo.MongoErrorCode.NoSuchTransaction;
import static signal.mongo.MongoErrorCode.WriteConflict;
import static signal.mongo.TxnResponse.ok;
import static signal.mongo.TxnResponse.parkThread;
import static signal.mongo.TxnResponse.retryableError;
import static signal.mongo.TxnResponse.thrownAnError;
import static signal.mongo.Utils.mappedHolder2AndFilter;
import static signal.mongo.Utils.parkCurrentThreadUntil;
import static signal.mongo.Utils.unparkSuccessor;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.DoNotCall;
import com.google.errorprone.annotations.Keep;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import org.bson.Document;
import org.bson.conversions.Bson;
import signal.api.DistributeMutexLock;
import signal.api.Holder;
import signal.api.Lease;
import signal.api.SignalException;

/**
 * DistributeMutexLock支持锁的可重入性质
 *
 * <p>数据存储格式
 *
 * <pre>{@code
 * {
 *    _id: 'Test-Mutex',
 *    v: Long('1'),
 *    o: [
 *      {
 *        hostname: 'E4981F',
 *        thread: Long('27'),
 *        lease: '04125035701132000',
 *        state: Long('1')
 *      }
 *    ]
 *  }
 * }</pre>
 *
 * <p>锁的公平性：非公平
 */
@ThreadSafe
@AutoService(DistributeMutexLock.class)
public final class DistributeMutexLockImp extends DistributeMongoSignalBase
    implements DistributeMutexLock {
  private final EventBus eventBus;
  private final ReentrantLock lock;
  private final Condition available;

  @Keep
  @GuardedBy("varHandle")
  @VisibleForTesting
  StateVars<Boolean> stateVars;

  private final VarHandle varHandle;

  public DistributeMutexLockImp(
      Lease lease, String key, MongoClient mongoClient, MongoDatabase db, EventBus eventBus) {
    super(lease, key, mongoClient, db, MUTEX_LOCK_NAMED);

    this.stateVars = new StateVars<>(false);
    try {
      varHandle =
          MethodHandles.lookup()
              .findVarHandle(DistributeMutexLockImp.class, "stateVars", StateVars.class);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException();
    }
    this.lock = new ReentrantLock();
    this.available = lock.newCondition();
    this.eventBus = eventBus;
    this.eventBus.register(this);
  }

  @CheckReturnValue
  @Override
  public boolean tryLock(Long waitTime, TimeUnit timeUnit) throws InterruptedException {
    Document holder = currHolder();
    BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
        (session, coll) -> {
          StateVars<Boolean> currState = (StateVars<Boolean>) varHandle.getAcquire(this);
          int currStateAddr = identityHashCode(currState);

          Document mutex = coll.find(session, eq("_id", this.getKey())).first();
          if (mutex == null) {
            holder.put("state", 1L);
            InsertOneResult insertOneResult =
                coll.insertOne(
                    session,
                    new Document("_id", this.getKey())
                        .append("v", 1L)
                        .append("o", ImmutableList.of(holder)));
            return insertOneResult.getInsertedId() != null ? ok() : retryableError();
          }

          long revision = mutex.getLong("v"), newRevision = revision + 1;
          List<Document> holders = mutex.getList("o", Document.class);
          boolean holdersEmpty = holders == null || holders.isEmpty();

          var filter = and(eq("_id", this.getKey()), eq("v", mutex.getLong("v")));

          // 其他线程占用锁资源 挂起当前线程
          Optional<Document> optional = extractHolder(mutex, holder);
          if (optional.isEmpty() && !holdersEmpty) {
            if (identityHashCode(
                    varHandle.compareAndExchangeRelease(this, currState, new StateVars<>(true)))
                == currStateAddr) {
              return parkThread();
            }
            return retryableError();
          }

          Bson update;
          if (holdersEmpty) {
            holder.put("state", 1L);
            update = combine(inc("v", 1L), set("o", ImmutableList.of(holder)));
          } else {
            update = combine(inc("v", 1L), inc("o.$.state", 1L));
          }
          return ((mutex = coll.findOneAndUpdate(session, filter, update, UPDATE_OPTIONS)) != null
                  && mutex.getLong("v") == newRevision
                  && extractHolder(mutex, holder).isPresent())
              ? ok()
              : retryableError();
        };

    long s = nanoTime(), waitTimeNanos = timeUnit.toNanos(waitTime);
    boolean timed = waitTime > 0;

    for (; ; ) {
      // 尝试着挂起当前线程
      boolean elapsed =
          parkCurrentThreadUntil(
              lock,
              available,
              () -> ((StateVars<Boolean>) varHandle.getAcquire(this)).value,
              timed,
              s,
              (waitTimeNanos - (nanoTime() - s)));
      if (!elapsed) return false;

      checkState();

      TxnResponse rsp =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                  WriteConflict, DuplicateKey, NoSuchTransaction),
              null,
              t -> !t.txnOk && t.retryable && !t.parkThread && !t.thrownError,
              timed,
              timed ? (waitTimeNanos - (nanoTime() - s)) : -1L,
              NANOSECONDS);
      if (rsp.txnOk) {
        return true;
      }
      if (rsp.thrownError) throw new SignalException(rsp.message);

      if (rsp.parkThread) Thread.onSpinWait();
    }
  }

  @Override
  public void unlock() {
    checkState();

    Document holder = currHolder();
    BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
        (session, coll) -> {
          Document mutex =
              coll.find(
                      session,
                      and(eq("_id", this.getKey()), elemMatch("o", mappedHolder2AndFilter(holder))))
                  .first();
          Optional<Document> optional;
          if (mutex == null || (optional = extractHolder(mutex, holder)).isEmpty())
            return thrownAnError("The current instance does not hold a mutex lock.");

          Document actualHolder = optional.get();
          long reversion = mutex.getLong("v"), newRevision = reversion + 1;
          long state = actualHolder.getLong("state"), newState = state - 1;

          var filter = and(eq("_id", this.getKey()), eq("v", reversion));
          if (actualHolder.getLong("state") <= 1L) {
            DeleteResult deleteResult = coll.deleteOne(session, filter);
            return deleteResult.getDeletedCount() == 1L ? ok() : retryableError();
          }

          var update = combine(inc("v", 1L), inc("o.$.state", -1L));
          return ((mutex = coll.findOneAndUpdate(session, filter, update, UPDATE_OPTIONS)) != null
                  && mutex.getLong("v") == newRevision
                  && (optional = extractHolder(mutex, holder)).isPresent()
                  && optional.get().getLong("state") == newState)
              ? ok()
              : retryableError();
        };
    TxnResponse rsp =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(NoSuchTransaction, WriteConflict),
            null,
            t -> !t.txnOk && t.retryable && !t.thrownError);
    if (rsp.thrownError) throw new SignalException(rsp.message);
  }

  @Override
  public boolean isLocked() {
    checkState();
    BiFunction<ClientSession, MongoCollection<Document>, Boolean> command =
        (session, coll) -> {
          long c = coll.countDocuments(session, and(eq("_id", this.getKey()), size("o", 1)));
          return c == 1L;
        };
    return commandExecutor.loopExecute(
        command, commandExecutor.defaultDBErrorHandlePolicy(NoSuchTransaction));
  }

  @Override
  public boolean isHeldByCurrentThread() {
    checkState();
    Document holder = currHolder();
    BiFunction<ClientSession, MongoCollection<Document>, Boolean> command =
        (session, coll) ->
            coll.countDocuments(
                    session,
                    and(eq("_id", this.getKey()), elemMatch("o", mappedHolder2AndFilter(holder))))
                > 0L;
    return commandExecutor.loopExecute(
        command, commandExecutor.defaultDBErrorHandlePolicy(NoSuchTransaction));
  }

  @Override
  public Holder getHolder() {
    checkState();
    return super.doGetFirstHolder();
  }

  @Override
  protected void doClose() {
    eventBus.unregister(this);
    unparkSuccessor(lock, available, true);
    forceUnlock();
  }

  @Override
  public long getEnterCount() {
    checkState();
    BiFunction<ClientSession, MongoCollection<Document>, Long> command =
        (session, coll) -> {
          Document enterCountDoc =
              coll.aggregate(
                      session,
                      ImmutableList.of(
                          match(eq("_id", this.getKey())),
                          unwind("$o"),
                          project(fields(excludeId(), computed("enterCount", "$o.state")))))
                  .first();
          return enterCountDoc == null
              ? 0L
              : enterCountDoc.get("enterCount", Number.class).longValue();
        };
    return commandExecutor.loopExecute(
        command, commandExecutor.defaultDBErrorHandlePolicy(NoSuchTransaction));
  }

  @DoNotCall
  @Subscribe
  void awakeHead(ChangeStreamEvents.MutexLockChangeAndRemoveEvent event) {
    if (!event.lockKey().equals(this.getKey())) return;

    boolean holdersEmpty =
        ofNullable(event.fullDocument())
            .map(t -> t.getList("o", Document.class))
            .map(List::size)
            .map(t -> t > 0)
            .orElse(true);

    // 当前锁资源仍然有其他先线程持有 则直接退出
    if (!holdersEmpty) return;

    for (; ; ) {
      StateVars<Integer> currState = (StateVars<Integer>) varHandle.getAcquire(this);
      int currStateAddr = identityHashCode(currState);
      if (identityHashCode(
              varHandle.compareAndExchangeRelease(this, currState, new StateVars<>(false)))
          == currStateAddr) {
        unparkSuccessor(lock, available, false);
        return;
      }
      Thread.onSpinWait();
    }
  }

  // TODO Force Unlock API
  private void forceUnlock() {
    BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
        (session, coll) -> {
          Document mutex =
              coll.find(
                      session,
                      and(
                          eq("_id", this.getKey()),
                          elemMatch("o", eq("lease", getLease().getLeaseID()))))
                  .first();
          if (mutex == null) return null;
          DeleteResult deleteResult =
              coll.deleteOne(session, and(eq("_id", this.getKey()), eq("v", mutex.getLong("v"))));
          return deleteResult.getDeletedCount() == 1L ? ok() : retryableError();
        };

    TxnResponse rsp =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(NoSuchTransaction, WriteConflict),
            null,
            t -> !t.txnOk && t.retryable);
    if (rsp.thrownError) throw new SignalException(rsp.message);
  }
}
