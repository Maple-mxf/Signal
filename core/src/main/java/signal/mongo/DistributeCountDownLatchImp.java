package signal.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.setOnInsert;
import static java.lang.System.identityHashCode;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static signal.mongo.CollectionNamed.COUNT_DOWN_LATCH_NAMED;
import static signal.mongo.CommonTxnResponse.ok;
import static signal.mongo.CommonTxnResponse.retryableError;
import static signal.mongo.CommonTxnResponse.thrownAnError;
import static signal.mongo.MongoErrorCode.LOCK_BUSY;
import static signal.mongo.MongoErrorCode.LOCK_FAILED;
import static signal.mongo.MongoErrorCode.LOCK_TIMEOUT;
import static signal.mongo.MongoErrorCode.NO_SUCH_TRANSACTION;
import static signal.mongo.MongoErrorCode.WRITE_CONFLICT;
import static signal.mongo.Utils.parkCurrentThreadUntil;
import static signal.mongo.Utils.unparkSuccessor;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
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
import com.mongodb.client.result.InsertOneResult;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import signal.api.DistributeCountDownLatch;
import signal.api.Lease;
import signal.api.SignalException;
import signal.mongo.pojo.CountDownLatchDocument;
import signal.mongo.pojo.CountDownLatchWaiterDocument;

/**
 * 数据存储格式
 *
 * <pre>{@code
 * [
 *    {
 *         _id: 'test-count-down-latch',
 *         c: 8,
 *         cc: 0,
 *         o: [ { lease: '20091433915734183' } ],
 *         v: Long('1')
 *     }
 * ]
 * }</pre>
 */
@ThreadSafe
@AutoService(DistributeCountDownLatch.class)
final class DistributeCountDownLatchImp extends DistributeMongoSignalBase<CountDownLatchDocument>
    implements DistributeCountDownLatch {
  private final int count;
  private final EventBus eventBus;

  private final ReentrantLock lock;
  private final Condition countDone;

  // 建立内存屏障
  // 确保写操作的顺序性：防止某些写操作被重新排序到屏障之前
  // 确保读操作的顺序性：防止某些读操作被重新排序到屏障之后。
  // 保证线程间的内存可见性：确保在某个线程中进行的写操作对其他线程是可见的。
  // Value代表是否完成
  @Keep
  @GuardedBy("varHandle")
  @VisibleForTesting
  StatefulVar<Integer> statefulVar;

  // Acquire 语义
  // 1. 保证当前线程在交换操作后，能够“获得”并看到所有其他线程在交换之前已经做出的更新。
  // 2. 确保当前线程执行该交换操作之后，后续的读取操作能够看到正确的共享数据状态。
  // Release 语义
  // 1. 保证当前线程在交换操作之前，对共享变量的所有写操作（即交换操作之前的写操作）都已经完成，并且这些写操作对其他线程是可见的。
  // 2. 确保当前线程执行该交换操作之前，所有对共享数据的写操作已经对其他线程“发布”，使得其他线程能够正确看到这些更新。
  private final VarHandle varHandle;

  @Override
  public Collection<?> getParticipants() {
    BiFunction<
            ClientSession,
            MongoCollection<CountDownLatchDocument>,
            List<CountDownLatchWaiterDocument>>
        command =
            (session, coll) -> {
              var filter = eq("_id", getKey());
              CountDownLatchDocument document = coll.find(filter).limit(1).first();
              return document == null ? Collections.emptyList() : document.waiters();
            };
    return commandExecutor.loopExecute(
        command,
        commandExecutor.defaultDBErrorHandlePolicy(
                LOCK_BUSY, LOCK_FAILED, LOCK_TIMEOUT, NO_SUCH_TRANSACTION),
        null,
        t -> false);
  }

  private record InitCountDownLatchTxnResponse(
      boolean success, boolean retryable, boolean thrownError, String message, int stateValue) {}

  DistributeCountDownLatchImp(
      Lease lease,
      String key,
      MongoClient mongoClient,
      MongoDatabase db,
      int count,
      EventBus eventBus) {
    super(lease, key, mongoClient, db, COUNT_DOWN_LATCH_NAMED);
    if (count <= 0) throw new IllegalArgumentException();
    this.count = count;
    this.lock = new ReentrantLock();
    this.countDone = lock.newCondition();
    try {
      this.varHandle =
          MethodHandles.lookup()
              .findVarHandle(DistributeCountDownLatchImp.class, "statefulVar", StatefulVar.class);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException(e);
    }

    CountDownLatchWaiterDocument thisWaiter = buildCurrentWaiter();
    BiFunction<
            ClientSession, MongoCollection<CountDownLatchDocument>, InitCountDownLatchTxnResponse>
        command =
            (session, coll) -> {
              CountDownLatchDocument cdl =
                  coll.findOneAndUpdate(
                      session,
                      eq("_id", this.getKey()),
                      combine(
                          setOnInsert("count", this.count),
                          setOnInsert("cc", 0),
                          setOnInsert("version", 1L),
                          addToSet("waiters", thisWaiter)),
                      UPSERT_OPTIONS);
              if (cdl == null) return new InitCountDownLatchTxnResponse(false, true, false, "", 0);
              if (cdl.count() != this.count)
                return new InitCountDownLatchTxnResponse(
                    false,
                    false,
                    true,
                    "Count down error. Because another process is using count down latch resources",
                    0);
              return new InitCountDownLatchTxnResponse(true, false, false, "", 0);
            };

    for (; ; ) {
      InitCountDownLatchTxnResponse rsp =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                      LOCK_FAILED, NO_SUCH_TRANSACTION, WRITE_CONFLICT),
              null,
              t -> !t.success && t.retryable && !t.thrownError);
      if (rsp.thrownError) throw new SignalException(rsp.message);
      if (rsp.success) {
        this.statefulVar = new StatefulVar<>(rsp.stateValue);
        break;
      }
    }

    this.eventBus = eventBus;
    this.eventBus.register(this);
  }

  private CountDownLatchWaiterDocument buildCurrentWaiter() {
    return new CountDownLatchWaiterDocument(
        Utils.getCurrentHostname(), this.lease.getLeaseID(), Utils.getCurrentThreadName());
  }

  @Override
  public int count() {
    return count;
  }

  @Override
  public void countDown() {
    CountDownLatchWaiterDocument thisWaiter = this.buildCurrentWaiter();
    BiFunction<ClientSession, MongoCollection<CountDownLatchDocument>, CommonTxnResponse> command =
        (session, coll) -> {
          CountDownLatchDocument cdl = coll.find(session, eq("_id", this.getKey())).first();
          if (cdl == null) {
            cdl = new CountDownLatchDocument(key, count, 1, ImmutableList.of(thisWaiter), 1L);
            InsertOneResult insertOneResult = coll.insertOne(session, cdl);
            return (insertOneResult.getInsertedId() != null && insertOneResult.wasAcknowledged())
                ? ok()
                : retryableError();
          }
          if (cdl.count() != this.count)
            return thrownAnError(
                "Count down error. Because another process is using count down latch resources");
          if (cdl.cc() > this.count) return thrownAnError("Count down exceed.");

          long version = cdl.version(), newVersion = version + 1;
          var filter = and(eq("_id", this.getKey()), eq("version", version));

          // 到达删除条件
          if (cdl.cc() + 1 == this.count) {
            DeleteResult deleteResult = coll.deleteOne(session, filter);
            return deleteResult.getDeletedCount() == 1L ? ok() : retryableError();
          }
          var updates = combine(inc("cc", 1), inc("version", 1), addToSet("waiters", thisWaiter));
          return ((cdl = collection.findOneAndUpdate(session, filter, updates, UPDATE_OPTIONS))
                      != null
                  && cdl.containWaiter(thisWaiter)
                  && cdl.version() == newVersion)
              ? ok()
              : retryableError();
        };

    for (; ; ) {
      CommonTxnResponse rsp =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                      LOCK_FAILED, NO_SUCH_TRANSACTION, WRITE_CONFLICT),
              null,
              t -> !t.txnOk && t.retryable && !t.thrownError);
      if (rsp.txnOk) break;
      if (rsp.thrownError) throw new SignalException(rsp.message);
    }
  }

  @Override
  public void await() throws InterruptedException {
    this.await(-1L, NANOSECONDS);
  }

  @Override
  public void await(long waitTime, TimeUnit timeUnit) throws InterruptedException {
    long s = nanoTime(), waitTimeNanos = timeUnit.toNanos(waitTime);
    boolean timed = waitTime > 0;
    boolean elapsed =
        parkCurrentThreadUntil(
            lock,
            countDone,
            () -> {
              int v = ((StatefulVar<Integer>) varHandle.getAcquire(this)).value;
              return v < this.count;
            },
            timed,
            s,
            (waitTimeNanos - (nanoTime() - s)));
    if (!elapsed) throw new SignalException("Timeout.");
  }

  @Override
  protected void doClose() {
    this.eventBus.unregister(this);
    unparkSuccessor(lock, countDone, true);
  }

  @DoNotCall
  @Subscribe
  void awakeAllSuccessor(ChangeStreamEvents.CountDownLatchChangeEvent event) {
    if (!this.getKey().equals(event.cdlKey()) || this.count != event.c()) return;

    Next:
    for (; ; ) {
      StatefulVar<Integer> currState = (StatefulVar<Integer>) varHandle.getAcquire(this);
      int currStateHashCode = identityHashCode(currState);

      // 代表删除操作
      if (event.fullDocument() == null) {
        if (identityHashCode(
                varHandle.compareAndExchangeRelease(this, currState, new StatefulVar<>(this.count)))
            == currStateHashCode) {
          unparkSuccessor(lock, countDone, true);
          return;
        }
        Thread.onSpinWait();
        continue Next;
      }
      int cc = event.cc();
      if (identityHashCode(
              varHandle.compareAndExchangeRelease(this, currState, new StatefulVar<>(cc)))
          == currStateHashCode) {
        return;
      }
      Thread.onSpinWait();
      continue Next;
    }
  }
}
