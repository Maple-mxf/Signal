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
import static signal.mongo.MongoErrorCode.LockFailed;
import static signal.mongo.MongoErrorCode.NoSuchTransaction;
import static signal.mongo.MongoErrorCode.WriteConflict;
import static signal.mongo.TxnResponse.ok;
import static signal.mongo.TxnResponse.retryableError;
import static signal.mongo.TxnResponse.thrownAnError;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import org.bson.Document;
import signal.api.DistributeCountDownLatch;
import signal.api.Holder;
import signal.api.Lease;
import signal.api.SignalException;

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
final class DistributeCountDownLatchImp extends DistributeMongoSignalBase
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
  StatefulVar<Integer> stateVars;

  // Acquire 语义
  // 1. 保证当前线程在交换操作后，能够“获得”并看到所有其他线程在交换之前已经做出的更新。
  // 2. 确保当前线程执行该交换操作之后，后续的读取操作能够看到正确的共享数据状态。
  // Release 语义
  // 1. 保证当前线程在交换操作之前，对共享变量的所有写操作（即交换操作之前的写操作）都已经完成，并且这些写操作对其他线程是可见的。
  // 2. 确保当前线程执行该交换操作之前，所有对共享数据的写操作已经对其他线程“发布”，使得其他线程能够正确看到这些更新。
  private final VarHandle varHandle;

  DistributeCountDownLatchImp(
      Lease lease,
      String key,
      MongoClient mongoClient,
      MongoDatabase db,
      int count,
      EventBus eventBus) {
    super(lease, key, mongoClient, db, COUNT_DOWN_LATCH_NAMED);
    if (count <= 0)
      throw new IllegalArgumentException("The value of count must be greater than 0.");
    this.count = count;
    this.lock = new ReentrantLock();
    this.countDone = lock.newCondition();

    this.eventBus = eventBus;
    this.eventBus.register(this);

    try {
      this.varHandle =
          MethodHandles.lookup()
              .findVarHandle(DistributeCountDownLatchImp.class, "stateVars", StatefulVar.class);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException();
    }

    Document h = currHolder();
    BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
        (session, coll) -> {
          Document cdl =
              coll.findOneAndUpdate(
                  session,
                  eq("_id", this.getKey()),
                  combine(
                      setOnInsert("c", this.count),
                      setOnInsert("cc", 0),
                      setOnInsert("v", 1L),
                      addToSet("o", h)),
                  FU_UPSERT_OPTIONS);
          if (cdl == null) return retryableError();
          if (cdl.getInteger("c") != this.count)
            return thrownAnError(
                "Count down error. Because another process is using count down latch resources");
          this.stateVars = new StatefulVar<>(cdl.getInteger("cc"));
          return ok();
        };

    for (; ; ) {
      TxnResponse rsp =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                  LockFailed, NoSuchTransaction, WriteConflict),
              null,
              t -> !t.txnOk && t.retryable && !t.thrownError);
      if (rsp.txnOk) return;
      if (rsp.thrownError) throw new SignalException(rsp.message);
    }
  }

  @Override
  public int count() {
    return count;
  }

  Document currHolder() {
    return new Document("lease", this.getLease().getLeaseID());
  }

  Optional<Document> extractHolder(Document signal, Document holder) {
    List<Document> holders = signal.getList("o", Document.class);
    if (holders == null || holders.isEmpty()) return Optional.empty();
    return holders.stream().filter(t -> t.get("lease").equals(holder.get("lease"))).findFirst();
  }

  @Override
  public void countDown() {
    Document holder = currHolder();

    BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
        (session, coll) -> {
          Document cdl = coll.find(session, eq("_id", this.getKey())).first();
          if (cdl == null) {
            InsertOneResult insertOneResult =
                coll.insertOne(
                    new Document("_id", this.getKey())
                        .append("c", this.count)
                        .append("cc", 1)
                        .append(
                            "o",
                            ImmutableList.of(new Document("lease", this.getLease().getLeaseID())))
                        .append("v", 1L));
            return (insertOneResult.getInsertedId() != null && insertOneResult.wasAcknowledged())
                ? ok()
                : retryableError();
          }
          int cc = cdl.getInteger("cc");
          if (cdl.getInteger("c") != this.count)
            return thrownAnError(
                "Count down error. Because another process is using count down latch resources");
          if (cc > this.count) return thrownAnError("Count down exceed.");

          long revision = cdl.getLong("v"), newRevision = revision + 1;
          var filter = and(eq("_id", this.getKey()), eq("v", revision));

          // 到达删除条件
          if (cc + 1 == this.count) {
            DeleteResult deleteResult = coll.deleteOne(session, filter);
            return deleteResult.getDeletedCount() == 1L ? ok() : retryableError();
          }
          var updates = combine(inc("cc", 1), inc("v", 1), addToSet("o", holder));
          return ((cdl = collection.findOneAndUpdate(session, filter, updates, FU_UPDATE_OPTIONS))
                      != null
                  && extractHolder(cdl, holder).isPresent()
                  && cdl.getLong("v") == newRevision)
              ? ok()
              : retryableError();
        };
    TxnResponse rsp =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(
                LockFailed, NoSuchTransaction, WriteConflict),
            null,
            t -> !t.txnOk && t.retryable && !t.thrownError);
    if (rsp.txnOk) return;
    if (rsp.thrownError) throw new SignalException(rsp.message);
    throw new SignalException("Unknown Error.");
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
      int currStateAddr = identityHashCode(currState);

      // 代表删除操作
      if (event.fullDocument() == null) {
        if (identityHashCode(
                varHandle.compareAndExchangeRelease(this, currState, new StatefulVar<>(this.count)))
            == currStateAddr) {
          unparkSuccessor(lock, countDone, true);
          return;
        }
        Thread.onSpinWait();
        continue Next;
      }
      int cc = event.cc();
      if (identityHashCode(
              varHandle.compareAndExchangeRelease(this, currState, new StatefulVar<>(cc)))
          == currStateAddr) {
        return;
      }
      Thread.onSpinWait();
      continue Next;
    }
  }

  @Override
  public Collection<Holder> getHolders() {
    return super.doGetHolders();
  }
}
