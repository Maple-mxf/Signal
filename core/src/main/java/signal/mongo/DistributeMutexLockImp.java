package signal.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static java.lang.System.nanoTime;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static signal.mongo.CollectionNamed.MUTEX_LOCK_NAMED;
import static signal.mongo.MongoErrorCode.DUPLICATE_KEY;
import static signal.mongo.MongoErrorCode.NO_SUCH_TRANSACTION;
import static signal.mongo.MongoErrorCode.WRITE_CONFLICT;
import static signal.mongo.Utils.getCurrentHostname;
import static signal.mongo.Utils.getCurrentThreadName;
import static signal.mongo.Utils.parkCurrentThreadUntil;
import static signal.mongo.Utils.unparkSuccessor;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import org.bson.Document;
import signal.api.DistributeMutexLock;
import signal.api.Lease;
import signal.api.SignalException;
import signal.mongo.pojo.MutexLockDocument;
import signal.mongo.pojo.MutexLockOwnerDocument;

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
 *      }
 *    ]
 *  }
 * }</pre>
 *
 * <p>锁的公平性：非公平
 */
@ThreadSafe
@AutoService(DistributeMutexLock.class)
public final class DistributeMutexLockImp extends DistributeMongoSignalBase<MutexLockDocument>
    implements DistributeMutexLock {
  private final EventBus eventBus;
  private final ReentrantLock lock;
  private final Condition available;

  /**
   * {@link StatefulVar#value}存储的是{@link MutexLockOwnerDocument#hashCode()}， 当{@link
   * StatefulVar#value} 等于 null时，代表当前未有任何线程获取到锁
   */
  @Keep
  @GuardedBy("varHandle")
  @VisibleForTesting
  StatefulVar<Integer> state;

  private final VarHandle varHandle;

  public DistributeMutexLockImp(
      Lease lease, String key, MongoClient mongoClient, MongoDatabase db, EventBus eventBus) {
    super(lease, key, mongoClient, db, MUTEX_LOCK_NAMED);
    this.state = new StatefulVar<>(null);
    try {
      varHandle =
          MethodHandles.lookup()
              .findVarHandle(DistributeMutexLockImp.class, "state", StatefulVar.class);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException();
    }
    this.lock = new ReentrantLock();
    this.available = lock.newCondition();
    this.eventBus = eventBus;
    this.eventBus.register(this);
  }

  private MutexLockOwnerDocument buildCurrentOwner() {
    return new MutexLockOwnerDocument(
        getCurrentHostname(), lease.getId(), getCurrentThreadName(), 1);
  }

  @CanIgnoreReturnValue
  private boolean safeUpdateState(StatefulVar<Integer> captureState, Integer newStateValue) {
    StatefulVar<Integer> newState = new StatefulVar<>(newStateValue);
    if (captureState.equals(newState)) return true;
    return varHandle.compareAndExchangeRelease(
            DistributeMutexLockImp.this, captureState, new StatefulVar<>(newStateValue))
        == captureState;
  }

  private record TryLockTxnResponse(
      boolean tryLockSuccess,
      boolean retryable,
      boolean cas,
      StatefulVar<Integer> captureState,
      Integer newStateValue) {}

  private record UnlockTxnResponse(
      boolean unlockSuccess,
      boolean retryable,
      String exceptionMessage,
      boolean cas,
      StatefulVar<Integer> captureState,
      Integer newStateValue) {}

  /**
   * TODO 需要斟酌在TryLock函数内部是否有必要更新lockOwner的值,如果TryLock函数内部会更新LockOwner的值， TODO
   * 则可能会带来mutex的enterCount字段的无限自增，在事物成功的情况但是变量更新失败的情况下会带来这个问题
   *
   * <p>{@link DistributeMutexLockImp#state} == null,其他线程已经得到了锁，但未接受到ChangeStream事件
   *
   * <p>第一种情况：tryLock 先更新了{@link DistributeMutexLockImp#state}的值，lockOwner != null, 线程成功挂起
   *
   * <p>第二种情况：{@link DistributeMutexLockImp#awakeSuccessor(ChangeEvents.MutexLockChangeEvent)}
   * 先更新了{@link DistributeMutexLockImp#state} 的值，lockOwner != null, TryLock的线程更新{@link
   * DistributeMutexLockImp#state}失败，重新进入TryLock进程
   *
   * @param waitTime 等待时间
   * @param timeUnit 时间单位
   * @return 是否得到锁
   * @throws InterruptedException
   */
  @CheckReturnValue
  @Override
  public boolean tryLock(Long waitTime, TimeUnit timeUnit) throws InterruptedException {
    MutexLockOwnerDocument thisOwner = buildCurrentOwner();
    BiFunction<ClientSession, MongoCollection<MutexLockDocument>, TryLockTxnResponse> command =
        (session, coll) -> {
          @SuppressWarnings("unchecked")
          StatefulVar<Integer> captureState = (StatefulVar<Integer>) varHandle.getAcquire(this);

          MutexLockDocument mutex = coll.find(session, eq("_id", this.getKey())).first();
          if (mutex == null) {
            mutex = new MutexLockDocument(key, thisOwner, 1L);
            InsertOneResult insertOneResult = coll.insertOne(session, mutex);
            boolean success =
                insertOneResult.wasAcknowledged() && insertOneResult.getInsertedId() != null;
            return new TryLockTxnResponse(
                success, !success, success, captureState, success ? thisOwner.hashCode() : null);
          }

          long version = mutex.version(), newVersion = version + 1;

          // 其他线程占用锁资源 挂起当前线程, 判断是否属于当前线程
          if (mutex.owner() != null) {
            if (!mutex.owner().equals(thisOwner))
              return new TryLockTxnResponse(
                  false, false, true, captureState, mutex.owner().hashCode());
            mutex =
                coll.findOneAndUpdate(
                    session,
                    and(eq("_id", key), eq("version", version)),
                    combine(inc("version", 1L), inc("owner.enter_count", 1)),
                    UPDATE_OPTIONS);
            boolean success = mutex != null && mutex.version() == newVersion;
            return new TryLockTxnResponse(
                success, !success, success, captureState, success ? thisOwner.hashCode() : null);
          }
          mutex =
              coll.findOneAndUpdate(
                  session,
                  and(eq("_id", key), eq("version", version)),
                  combine(inc("version", 1L), set("owner", thisOwner)),
                  UPDATE_OPTIONS);
          boolean success =
              (mutex != null && mutex.version() == newVersion && thisOwner.equals(mutex.owner()));
          return new TryLockTxnResponse(
              success, !success, success, captureState, success ? thisOwner.hashCode() : null);
        };

    long s = nanoTime(), waitTimeNanos = timeUnit.toNanos(waitTime);
    boolean timed = waitTime > 0;

    Next:
    for (; ; ) {
      checkState();

      // 尝试着挂起当前线程
      boolean elapsed =
          parkCurrentThreadUntil(
              lock,
              available,
              () -> {
                @SuppressWarnings("unchecked")
                StatefulVar<Integer> sv = (StatefulVar<Integer>) varHandle.getAcquire(this);
                if (sv.value == null) return false;
                return !sv.value.equals(thisOwner.hashCode());
              },
              timed,
              s,
              (waitTimeNanos - (nanoTime() - s)));
      if (!elapsed) return false;

      TryLockTxnResponse rsp =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                  WRITE_CONFLICT, DUPLICATE_KEY, NO_SUCH_TRANSACTION),
              null,
              t -> t.retryable,
              timed,
              timed ? (waitTimeNanos - (nanoTime() - s)) : -1L,
              NANOSECONDS);

      // 此处忽略CAS失败的情况
      if (rsp.cas) safeUpdateState(rsp.captureState, rsp.newStateValue);

      if (rsp.tryLockSuccess) return true;
      if (rsp.retryable) {
        Thread.onSpinWait();
        continue Next;
      }
    }
  }

  @Override
  public void unlock() {
    MutexLockOwnerDocument thisOwner = buildCurrentOwner();
    BiFunction<ClientSession, MongoCollection<MutexLockDocument>, UnlockTxnResponse> command =
        (session, coll) -> {
          @SuppressWarnings("unchecked")
          StatefulVar<Integer> captureState = (StatefulVar<Integer>) varHandle.getAcquire(this);

          MutexLockDocument mutex = coll.find(session, eq("_id", key)).first();
          if (mutex == null || mutex.owner() == null || !mutex.owner().equals(thisOwner))
            return new UnlockTxnResponse(
                false,
                false,
                "The current instance does not hold a mutex lock.",
                false,
                captureState,
                null);

          MutexLockOwnerDocument thatOwner = mutex.owner();
          if (thatOwner.enterCount() <= 1) {
            DeleteResult deleteResult =
                coll.deleteOne(session, and(eq("_id", key), eq("version", mutex.version())));
            boolean success =
                deleteResult.wasAcknowledged() && deleteResult.getDeletedCount() == 1L;
            return new UnlockTxnResponse(success, !success, null, success, captureState, null);
          }
          long version = mutex.version(), newVersion = version + 1L;

          boolean success =
              (mutex =
                          coll.findOneAndUpdate(
                              session,
                              and(eq("_id", key), eq("version", version)),
                              combine(inc("version", 1L), inc("owner.enter_count", -1)),
                              UPDATE_OPTIONS))
                      != null
                  && mutex.version() == newVersion;
          return new UnlockTxnResponse(
              success,
              !success,
              null,
              success,
              captureState,
              mutex == null ? null : mutex.owner().hashCode());
        };

    Next:
    for (; ; ) {
      checkState();
      UnlockTxnResponse rsp =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(NO_SUCH_TRANSACTION, WRITE_CONFLICT),
              null,
              t -> !t.unlockSuccess && t.retryable);

      if (rsp.unlockSuccess) break Next;
      if (rsp.exceptionMessage != null && !rsp.exceptionMessage.isEmpty())
        throw new SignalException(rsp.exceptionMessage);
      Thread.onSpinWait();
    }
  }

  @Override
  public boolean isLocked() {
    checkState();
    BiFunction<ClientSession, MongoCollection<MutexLockDocument>, Boolean> command =
        (session, coll) ->
            coll.countDocuments(session, and(eq("_id", this.getKey()), exists("owner", true))) > 0L;
    return commandExecutor.loopExecute(command);
  }

  @Override
  public boolean isHeldByCurrentThread() {
    checkState();
    MutexLockOwnerDocument thisOwner = buildCurrentOwner();
    BiFunction<ClientSession, MongoCollection<MutexLockDocument>, Boolean> command =
        (session, coll) ->
            coll.countDocuments(
                    session,
                    and(
                        eq("_id", this.getKey()),
                        eq("owner.hostname", thisOwner.hostname()),
                        eq("owner.lease", thisOwner.lease()),
                        eq("owner.thread", thisOwner.thread())))
                > 0L;
    return commandExecutor.loopExecute(command);
  }

  @Override
  public Object getOwner() {
    checkState();
    BiFunction<ClientSession, MongoCollection<MutexLockDocument>, MutexLockOwnerDocument> command =
        (session, coll) -> {
          MutexLockDocument mutex =
              coll.find(session, eq("_id", this.getKey()))
                  .projection(fields(include("owner"), excludeId()))
                  .first();
          return mutex == null ? null : mutex.owner();
        };
    return commandExecutor.loopExecute(command);
  }

  @Override
  protected void doClose() {
    eventBus.unregister(this);
    unparkSuccessor(lock, available, true);
    forceUnlock();
  }

  @DoNotCall
  @Subscribe
  void awakeSuccessor(ChangeEvents.MutexLockChangeEvent event) {
    if (!event.key().equals(this.getKey())) return;
    Next:
    for (; ; ) {
      @SuppressWarnings("unchecked")
      StatefulVar<Integer> captureState = (StatefulVar<Integer>) varHandle.getAcquire(this);

      Document fullDocument = event.fullDocument();
      if (fullDocument == null) {
        if (safeUpdateState(captureState, null)) {
          unparkSuccessor(lock, available, false);
          return;
        }
        Thread.onSpinWait();
        continue Next;
      }
      Integer thatOwnerHashCode =
          ofNullable(fullDocument.get("owner", Document.class))
              .map(
                  t ->
                      new MutexLockOwnerDocument(
                          t.getString("hostname"),
                          t.getString("lease"),
                          t.getString("thread"),
                          t.getInteger("enter_count")))
              .map(MutexLockOwnerDocument::hashCode)
              .orElse(null);
      if (safeUpdateState(captureState, thatOwnerHashCode)) {
        unparkSuccessor(lock, available, false);
        return;
      }
      Thread.onSpinWait();
      continue Next;
    }
  }

  private void forceUnlock() {
    BiFunction<ClientSession, MongoCollection<MutexLockDocument>, Boolean> command =
        (session, coll) -> {
          MutexLockDocument mutex =
              coll.find(session, and(eq("_id", this.getKey()), eq("owner.lease", lease.getId())))
                  .first();
          if (mutex == null) return true;
          DeleteResult deleteResult =
              coll.deleteOne(
                  session, and(eq("_id", this.getKey()), eq("version", mutex.version())));
          return deleteResult.getDeletedCount() == 1L && deleteResult.wasAcknowledged();
        };

    Boolean forceUnlockRsp =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(NO_SUCH_TRANSACTION, WRITE_CONFLICT),
            null,
            t -> !t);
  }
}
