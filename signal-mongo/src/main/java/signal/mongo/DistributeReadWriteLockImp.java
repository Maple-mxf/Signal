package signal.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.size;
import static com.mongodb.client.model.Filters.type;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.pull;
import static com.mongodb.client.model.Updates.set;
import static java.lang.System.identityHashCode;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static signal.mongo.CollectionNamed.READ_WRITE_LOCK_NAMED;
import static signal.mongo.MongoErrorCode.DuplicateKey;
import static signal.mongo.MongoErrorCode.LockFailed;
import static signal.mongo.MongoErrorCode.NoSuchTransaction;
import static signal.mongo.MongoErrorCode.TransactionExceededLifetimeLimitSeconds;
import static signal.mongo.MongoErrorCode.WriteConflict;
import static signal.mongo.CommonTxnResponse.ok;
import static signal.mongo.CommonTxnResponse.parkThread;
import static signal.mongo.CommonTxnResponse.retryableError;
import static signal.mongo.CommonTxnResponse.thrownAnError;
import static signal.mongo.Utils.mappedHolder2AndFilter;
import static signal.mongo.Utils.parkCurrentThreadUntil;

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
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import org.bson.BsonType;
import org.bson.Document;
import signal.api.DistributeLock;
import signal.api.DistributeReadWriteLock;
import signal.api.Holder;
import signal.api.Lease;
import signal.api.SignalException;

/**
 * 数据存储格式
 *
 * <pre>{@code
 * {
 *   _id: 'TestReadWriteLock',
 *   v: Long('10'),
 *   m: 'r',
 *   o: [
 *     {
 *       hostname: '2ADBEA',
 *       thread: Long('28'),
 *       lease: '02121104938650000'
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <p>锁的公平性：非公平锁
 */
@AutoService({DistributeReadWriteLock.class})
public final class DistributeReadWriteLockImp extends DistributeMongoSignalBase
    implements DistributeReadWriteLock {

  // TODO 锁的可重入性
  private final DistributeReadLockImp readLockImp;
  // TODO 锁的可重入性
  private final DistributeWriteLockImp writeLockImp;

  /**
   * 当前锁的可用状态标识，state的值为NULL或者 "" 空字符串则代表当前锁的资源可用 若为"r" 或者 "w" 表示，不可用会导致执行{@link
   * DistributeLock#lock()}操作的线程被挂起。
   *
   * <p>volatile会在内存中创建一个内存屏障，这个屏障可以保证修改的顺序性
   *
   * <ul>
   *   <li>1: 空或者空字符串代表获取写锁的线程或者获取读锁的线程都可以尝试获取锁资源
   *   <li>2: r代表当前锁处于读锁占领资源
   *   <li>3: w代表当前锁处于写锁占领资源
   * </ul>
   *
   * <p>state的值会被三个位置并发修改
   *
   * <ul>
   *   <li>来自MongoDB ChangeStream的事件通知，此方法本身不会并发{@link
   *       DistributeReadWriteLockImp#awakeSuccessor(ChangeStreamEvents.ReadWriteLockChangeAndRemovedEvent)}
   *   <li>{@link DistributeWriteLockImp#lock()}
   *   <li>{@link DistributeReadLockImp#lock()}
   * </ul>
   *
   * <p>当前锁的state的值 == "r",
   *
   * <ul>
   *   <li>A操作: Java线程1 执行了{@link DistributeReadLockImp#unlock()}操作
   *   <li>B操作: Java线程2 执行了{@link DistributeWriteLockImp#lock()}操作
   *   <li>C操作: Java线程3 执行了{@link
   *       DistributeReadWriteLockImp#awakeSuccessor(ChangeStreamEvents.ReadWriteLockChangeAndRemovedEvent)}
   * </ul>
   *
   * <p>A操作先执行释放锁资源，假设 B操作和C操作同时进入并发临界区，B操作和C操作都会更新 state 的值，只能有一个操作可以更新state的值成功， 因为存在{@link
   * AtomicLong#compareAndSet(long, long)}的CAS机制，所以会出现以下两种场景
   *
   * <ul>
   *   <li>C操作对应的线程更新state成功， C操作将state的值更新为 "" 空字符串，表示当前锁资源可用，B操作更新state失败，会在事务内部返回{@link
   *       CommonTxnResponse#retryableError()}，C操作进行重试，读到当前锁资源可用，整个过程不存在脏读或者{@link
   *       DistributeReadWriteLockImp#awakeSuccessor(ChangeStreamEvents.ReadWriteLockChangeAndRemovedEvent)}函数错过没有挂起的线程
   *   <li>B操作对应的线程更新state成功，假设此时B操作读到了脏数据（A操作提交之前的数据版本）， B操作将state的值更新为 "r"，表示当前锁资源不可用，B操作挂起线程，
   *       C操作唤醒挂起的线程，整个过程即使存在脏读的情况，但是依然会保证结果的正确性
   * </ul>
   */
  @Keep
  @GuardedBy("varHandle")
  @VisibleForTesting
  final StatefulVar<String> stateVars;

  private final VarHandle varHandle;

  private final EventBus eventBus;

  private final ReentrantLock lock;
  private final Condition available;

  private static final String READ_MODE = "r";
  private static final String WRITE_MODE = "w";

  DistributeReadWriteLockImp(
      Lease lease, String key, MongoClient mongoClient, MongoDatabase db, EventBus eventBus) {
    super(lease, key, mongoClient, db, READ_WRITE_LOCK_NAMED);

    this.stateVars = new StatefulVar<>("");
    try {
      varHandle =
          MethodHandles.lookup()
              .findVarHandle(DistributeReadWriteLockImp.class, "stateVars", StatefulVar.class);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException(e);
    }

    this.eventBus = eventBus;
    this.lock = new ReentrantLock();
    this.available = lock.newCondition();

    this.eventBus.register(this);
    this.readLockImp = new DistributeReadLockImp(lease, key, mongoClient, db);
    this.writeLockImp = new DistributeWriteLockImp(lease, key, mongoClient, db);
  }

  @Override
  public DistributeReadLock readLock() {
    return readLockImp;
  }

  @Override
  public DistributeWriteLock writeLock() {
    return writeLockImp;
  }

  @Override
  protected void doClose() {
    eventBus.unregister(this);
    unparkSuccessor(true);
    readLockImp.close();
    writeLockImp.close();
    forceUnlock();
  }

  @ThreadSafe
  @AutoService(DistributeWriteLock.class)
  private class DistributeWriteLockImp extends DistributeMongoSignalBase
      implements DistributeWriteLock {

    public DistributeWriteLockImp(
        Lease lease, String key, MongoClient mongoClient, MongoDatabase db) {
      super(lease, key, mongoClient, db, READ_WRITE_LOCK_NAMED);
    }

    @CheckReturnValue
    @Override
    public boolean tryLock(Long waitTime, TimeUnit timeUnit) throws InterruptedException {
      Document holder = currHolder();

      BiFunction<ClientSession, MongoCollection<Document>, CommonTxnResponse> command =
          (session, coll) -> {

            // 执行之前先获取state的值，避免和wakeHead函数并发更新state的值冲突
            StatefulVar<String> currState =
                (StatefulVar<String>) varHandle.getAcquire(DistributeWriteLockImp.this);

            Document writeLock = coll.find(session, eq("_id", this.getKey())).first();
            if (writeLock == null) {
              InsertOneResult insertOneResult =
                  coll.insertOne(
                      session,
                      new Document("_id", this.getKey())
                          .append("m", WRITE_MODE)
                          .append("v", 1L)
                          .append("o", ImmutableList.of(holder)));
              return insertOneResult.getInsertedId() != null ? ok() : retryableError();
            }
            List<Document> holders = writeLock.getList("o", Document.class);
            boolean inReadMode = READ_MODE.equals(writeLock.getString("m"));
            boolean holdersEmpty = holders == null || holders.isEmpty();

            if (inReadMode && !holdersEmpty) {
              // 设置当前锁的状态处于r状态
              if (identityHashCode(currState)
                  == identityHashCode(
                      varHandle.compareAndExchangeRelease(
                          DistributeReadWriteLockImp.this, currState, new StatefulVar<>("r")))) {
                return parkThread();
              }
              return retryableError();
            }

            long revision = writeLock.getLong("v");
            long newRevision = revision + 1;
            var filter = and(eq("_id", this.getKey()), eq("v", revision));
            var update =
                inReadMode
                    ? combine(set("m", WRITE_MODE), inc("v", 1L), addToSet("o", holder))
                    : combine(inc("v", 1L), addToSet("o", holder));
            // 如果为空代表无法匹配到对应的锁资源，需要继续重试
            return ((writeLock = coll.findOneAndUpdate(session, filter, update, UPDATE_OPTIONS))
                        != null
                    && writeLock.getLong("v") == newRevision
                    && extractHolder(writeLock, holder).isPresent())
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
                () -> {
                  @SuppressWarnings("UnnecessaryParentheses")
                  StatefulVar<String> sv =
                      (StatefulVar<String>) varHandle.getAcquire(DistributeReadWriteLockImp.this);
                  return !sv.value.isEmpty();
                },
                timed,
                s,
                (waitTimeNanos - (nanoTime() - s)));

        if (!elapsed) throw new SignalException("Timeout.");

        checkState();

        CommonTxnResponse outbound =
            commandExecutor.loopExecute(
                command,
                commandExecutor.defaultDBErrorHandlePolicy(
                    NoSuchTransaction,
                    WriteConflict,
                    LockFailed,
                    TransactionExceededLifetimeLimitSeconds,
                    DuplicateKey),
                null,
                t -> !t.txnOk && t.retryable && !t.thrownError && !t.parkThread,
                timed,
                timed ? (waitTimeNanos - (nanoTime() - s)) : -1L,
                NANOSECONDS);

        // 写锁和读锁不同的是这里不会在唤醒successor线程，因为独占的特性
        if (outbound.txnOk) {
          return true;
        }

        if (outbound.thrownError) {
          // 可能在事务提交阶段出现异常，所以需要判断lock的状态
          if (lock.isLocked() && lock.isHeldByCurrentThread()) lock.unlock();
          throw new SignalException(outbound.message);
        }
        if (outbound.parkThread) {
          Thread.onSpinWait();
        }
      }
    }

    @Override
    public void unlock() {
      checkState();
      Document holder = currHolder();

      BiFunction<ClientSession, MongoCollection<Document>, CommonTxnResponse> command =
          (session, coll) -> {
            DeleteResult deleteResult =
                coll.deleteOne(
                    and(
                        eq("_id", this.getKey()),
                        eq("m", WRITE_MODE),
                        type("o", BsonType.ARRAY),
                        size("o", 1),
                        elemMatch("o", mappedHolder2AndFilter(holder))));

            System.err.println(
                and(
                        eq("_id", this.getKey()),
                        eq("m", WRITE_MODE),
                        type("o", BsonType.ARRAY),
                        size("o", 1),
                        elemMatch("o", mappedHolder2AndFilter(holder)))
                    .toBsonDocument()
                    .toJson());

            return deleteResult.getDeletedCount() == 1
                ? ok()
                : thrownAnError("The current instance does not hold a write lock.");
          };
      CommonTxnResponse txnResponse =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                  NoSuchTransaction, WriteConflict, LockFailed),
              null,
              t -> false);
      if (txnResponse.thrownError) throw new SignalException(txnResponse.message);
    }

    @Override
    public boolean isLocked() {
      return super.doGetHolders() != null;
    }

    @Override
    public boolean isHeldByCurrentThread() {
      return super.doIsHeldCurrentThread();
    }

    @Override
    public Holder getHolder() {
      return super.doGetFirstHolder();
    }

    @Override
    protected void doClose() {}
  }

  @ThreadSafe
  @AutoService(DistributeReadLock.class)
  class DistributeReadLockImp extends DistributeMongoSignalBase implements DistributeReadLock {

    DistributeReadLockImp(Lease lease, String key, MongoClient mongoClient, MongoDatabase db) {
      super(lease, key, mongoClient, db, READ_WRITE_LOCK_NAMED);
    }

    @CheckReturnValue
    @Override
    public boolean tryLock(Long waitTime, TimeUnit timeUnit) throws InterruptedException {
      Document holder = currHolder();
      BiFunction<ClientSession, MongoCollection<Document>, CommonTxnResponse> command =
          (session, coll) -> {

            // 执行之前先获取state revision的值，避免和wakeHead函数并发更新state的值冲突
            StatefulVar<String> currState =
                (StatefulVar<String>) varHandle.getAcquire(DistributeReadWriteLockImp.this);

            Document readLock = coll.find(session, eq("_id", this.getKey())).first();
            if (readLock == null) {
              InsertOneResult insertOneResult =
                  coll.insertOne(
                      session,
                      new Document("_id", this.getKey())
                          .append("v", 1L)
                          .append("m", READ_MODE)
                          .append("o", ImmutableList.of(holder)));
              return insertOneResult.getInsertedId() != null ? ok() : retryableError();
            }

            List<Document> holders = readLock.getList("o", Document.class);
            boolean holdersEmpty = (holders == null || holders.isEmpty());
            boolean inWriteMode = WRITE_MODE.equals(readLock.getString("m"));

            // 当前有写锁占用资源，外部需要挂起线程
            if (!holdersEmpty && inWriteMode) {
              // 设置当前锁的状态处于w状态
              if (identityHashCode(currState)
                  == identityHashCode(
                      varHandle.compareAndExchangeRelease(
                          DistributeReadWriteLockImp.this, currState, new StatefulVar<>("w")))) {
                return parkThread();
              }
              return retryableError();
            }

            long revision = readLock.getLong("v");
            long newRevision = revision + 1;

            var filter = and(eq("_id", this.getKey()), eq("v", revision));
            // 没有任何线程占用锁，但是当前模式标记的是写锁模式
            // 有线程占用锁，但是锁的模式是读锁模式
            // 没有任何线程占用锁，但是当前模式标记的是写锁模式
            // 有线程占用锁，但是锁的模式是读锁模式
            var update =
                inWriteMode
                    ?
                    // 没有任何线程占用锁，但是当前模式标记的是写锁模式
                    combine(addToSet("o", holder), set("m", WRITE_MODE), inc("v", 1L))
                    // 有线程占用锁，但是锁的模式是读锁模式
                    : combine(addToSet("o", holder), inc("v", 1L));
            readLock = coll.findOneAndUpdate(session, filter, update, UPDATE_OPTIONS);
            return (readLock != null
                    && newRevision == readLock.getLong("v")
                    && extractHolder(readLock, holder).isPresent())
                ? ok()
                : retryableError();
          };

      long s = nanoTime(), waitTimeNanos = timeUnit.toNanos(waitTime);
      boolean timed = waitTime > 0;

      for (; ; ) {
        // 尝试挂起线程
        boolean elapsed =
            parkCurrentThreadUntil(
                lock,
                available,
                () -> {
                  @SuppressWarnings("UnnecessaryParentheses")
                  StatefulVar<String> sv =
                      ((StatefulVar<String>) (varHandle.getAcquire(DistributeReadWriteLockImp.this)));
                  return sv == null || sv.value.isEmpty() || "r".equals(sv.value);
                },
                timed,
                s,
                (waitTimeNanos - (nanoTime() - s)));

        if (!elapsed) return false;

        // 循环检查状态
        checkState();

        CommonTxnResponse txnResponse =
            commandExecutor.loopExecute(
                command,
                commandExecutor.defaultDBErrorHandlePolicy(
                    NoSuchTransaction, WriteConflict, LockFailed, DuplicateKey),
                null,
                t -> !t.txnOk && t.retryable && !t.thrownError && !t.parkThread,
                timed,
                timed ? (waitTimeNanos - (nanoTime() - s)) : -1L,
                NANOSECONDS);

        // 如果当前线程读锁获取成功，尝试着唤醒下一个等待的线程
        if (txnResponse.txnOk) {
          unparkSuccessor(false);
          return true;
        }

        if (txnResponse.thrownError) {
          // 如果当前加锁了可能是事务运行成功，但是提交失败
          if (lock.isLocked() && lock.isHeldByCurrentThread()) lock.unlock();
          throw new SignalException(txnResponse.message);
        }

        if (txnResponse.parkThread) {
          Thread.onSpinWait();
        }
      }
    }

    @Override
    public void unlock() {
      checkState();
      Document holder = currHolder();

      BiFunction<ClientSession, MongoCollection<Document>, CommonTxnResponse> command =
          (session, coll) -> {
            Document readLock =
                coll.find(session, and(eq("_id", this.getKey()), eq("m", READ_MODE))).first();
            if (readLock == null || extractHolder(readLock, holder).isEmpty())
              return thrownAnError("The current instance does not hold a read lock.");

            Long revision = readLock.getLong("v");
            List<Document> holders = readLock.getList("o", Document.class);

            // 达到删除的条件
            var filter = and(eq("_id", this.getKey()), eq("m", READ_MODE), eq("v", revision));
            if (holders.size() == 1) {
              DeleteResult deleteResult = coll.deleteOne(session, filter);
              return deleteResult.getDeletedCount() == 1L ? ok() : retryableError();
            }

            long newRevision = revision + 1;
            var update =
                combine(
                    pull(
                        "o",
                        and(
                            eq("lease", holder.get("lease")),
                            eq("host", holder.get("host")),
                            eq("thread", holder.get("thread")))),
                    inc("v", 1L));
            readLock = coll.findOneAndUpdate(session, filter, update, UPDATE_OPTIONS);
            return (readLock != null
                    && readLock.getLong("v") == newRevision
                    && extractHolder(readLock, holder).isEmpty())
                ? ok()
                : retryableError();
          };

      CommonTxnResponse outbound =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                  NoSuchTransaction, WriteConflict, LockFailed),
              null,
              t -> false);

      if (outbound.thrownError) throw new SignalException(outbound.message);
    }

    @Override
    public boolean isLocked() {
      return super.doGetHolders() != null;
    }

    @Override
    public boolean isHeldByCurrentThread() {
      return super.doIsHeldCurrentThread();
    }

    @Override
    public Collection<Holder> getHolders() {
      return super.doGetHolders();
    }

    @Override
    protected void doClose() {}
  }

  /**
   * 唤醒一个或者所有的等待线程
   *
   * @param all 是否唤醒所有的
   */
  private void unparkSuccessor(boolean all) {
    lock.lock();
    try {
      if (all) available.signalAll();
      else available.signal();
    } finally {
      lock.unlock();
    }
  }

  private void forceUnlock() {
    BiFunction<ClientSession, MongoCollection<Document>, CommonTxnResponse> command =
        (session, coll) -> {
          Document lock = coll.find(session, eq("_id", this.getKey())).first();
          if (lock == null) return ok();

          List<Document> holders = lock.getList("o", Document.class);

          // 达到删除条件
          long revision = lock.getLong("v"), newRevision = revision + 1;
          var filter = and(eq("_id", this.getKey()), eq("v", revision));
          if (holders == null || holders.isEmpty()) {
            DeleteResult deleteResult = coll.deleteOne(session, filter);
            return deleteResult.getDeletedCount() == 1L ? ok() : retryableError();
          }

          // 将所有的Holder删除
          var update = pull("o", new Document("lease", this.getLease().getLeaseID()));
          return ((lock = coll.findOneAndUpdate(session, filter, update, UPDATE_OPTIONS)) != null
                  && lock.getLong("v") == newRevision
                  && ((holders = lock.getList("o", Document.class)) == null
                      || holders.isEmpty()
                      || holders.stream()
                          .noneMatch(
                              t -> t.getString("lease").equals(this.getLease().getLeaseID()))))
              ? ok()
              : retryableError();
        };

    CommonTxnResponse txnResponse =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(
                NoSuchTransaction, WriteConflict, LockFailed),
            null,
            t -> !t.txnOk && t.retryable);

    if (!txnResponse.txnOk) throw new SignalException("Unknown Error.");
  }

  /**
   * 满足条件会唤醒头部线程 并且将{@link DistributeWriteLockImp#stateVars} 设置为空或者"r"
   *
   * @param event 删除事件或者锁的修改事件
   */
  @DoNotCall
  @Subscribe
  void awakeSuccessor(ChangeStreamEvents.ReadWriteLockChangeAndRemovedEvent event) {
    Document fullDocument = event.fullDocument();

    List<Document> holders =
        Optional.ofNullable(fullDocument).map(t -> t.getList("o", Document.class)).orElse(null);

    Next:
    for (; ; ) {
      // 执行之前先获取state的值，避免和lock函数并发更新state的值冲突
      StatefulVar<String> currState =
          (StatefulVar<String>) varHandle.getAcquire(DistributeReadWriteLockImp.this);

      // 当fullDocument为空时，对应的锁数据被删除的操作
      if (fullDocument == null || holders == null || holders.isEmpty()) {

        if (identityHashCode(currState)
            == identityHashCode(
                varHandle.compareAndExchangeRelease(
                    DistributeReadWriteLockImp.this, currState, new StatefulVar<>("")))) {
          // 将锁的可用状态置空 代表可以执行读锁尝试 也可以执行写锁尝试
          unparkSuccessor(false);
          return;
        }

        Thread.onSpinWait();
        continue Next;
      }

      String mode = fullDocument.getString("m");
      if (READ_MODE.equals(mode) && !holders.isEmpty()) {
        if (identityHashCode(currState)
            == identityHashCode(
                varHandle.compareAndExchangeRelease(
                    DistributeReadWriteLockImp.this, currState, new StatefulVar<>("r")))) {
          // 在写锁模式下，将锁的可用状态设置为r，代表读锁线程可以尝试着获取锁资源
          unparkSuccessor(false);
          return;
        }
        Thread.onSpinWait();
        continue Next;
      }
    }
  }
}
