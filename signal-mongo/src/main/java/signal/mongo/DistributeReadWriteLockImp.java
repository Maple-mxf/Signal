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
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static signal.mongo.CollectionNamed.READ_WRITE_LOCK_NAMED;
import static signal.mongo.MongoErrorCode.DuplicateKey;
import static signal.mongo.MongoErrorCode.ExceededTimeLimit;
import static signal.mongo.MongoErrorCode.LockFailed;
import static signal.mongo.MongoErrorCode.NoSuchTransaction;
import static signal.mongo.MongoErrorCode.TransactionExceededLifetimeLimitSeconds;
import static signal.mongo.MongoErrorCode.WriteConflict;
import static signal.mongo.TxnResponse.ok;
import static signal.mongo.TxnResponse.parkThread;
import static signal.mongo.TxnResponse.retryableError;
import static signal.mongo.TxnResponse.thrownAnError;
import static signal.mongo.Utils.mappedHolder2AndFilter;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import org.bson.BsonType;
import org.bson.Document;
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
 */
@AutoService({DistributeReadWriteLock.class})
public final class DistributeReadWriteLockImp extends DistributeMongoSignalBase
    implements DistributeReadWriteLock {
  private final DistributeReadLockImp readLockImp;
  private final DistributeWriteLockImp writeLockImp;

  // 当前锁的可用状态标识
  // 1: 空或者空字符串代表获取写锁的线程或者获取读锁的线程都可以尝试获取锁资源
  // 2: r代表当前锁处于读锁占领资源
  private volatile String availableState;

  private final EventBus eventBus;

  private final ReentrantLock lock;
  private final Condition available;

  static final String READ_MODE = "r";
  static final String WRITE_MODE = "w";

  DistributeReadWriteLockImp(
      Lease lease, String key, MongoClient mongoClient, MongoDatabase db, EventBus eventBus) {
    super(lease, key, mongoClient, db, READ_WRITE_LOCK_NAMED);
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

  @AutoService(DistributeWriteLock.class)
  private class DistributeWriteLockImp extends DistributeMongoSignalBase
      implements DistributeWriteLock {

    public DistributeWriteLockImp(
        Lease lease, String key, MongoClient mongoClient, MongoDatabase db) {
      super(lease, key, mongoClient, db, READ_WRITE_LOCK_NAMED);
    }

    @Override
    public boolean tryLock(Long waitTime, TimeUnit timeUnit) throws InterruptedException {
      Document holder = currHolder();

      BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
          (session, coll) -> {
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
            boolean inReadMode = READ_MODE.equals(writeLock.getString("type"));
            boolean holdersEmpty = holders == null || holders.isEmpty();
            if (inReadMode && !holdersEmpty) {
              //lock.lock();
              return parkThread();
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
                    && writeLock.getLong("v") != newRevision
                    && extractHolder(writeLock, holder).isPresent())
                ? ok()
                : retryableError();
          };

      long s = nanoTime(), waitTimeNanos = timeUnit.toNanos(waitTime);
      boolean timed = waitTime > 0;
      for (; ; ) {
        // 尝试着挂起当前线程
        boolean elapsed = parkCurrentThread("", timed, s, waitTimeNanos);
        if (!elapsed) return false;

        checkState();

        TxnResponse outbound =
            commandExecutor.loopExecute(
                command,
                commandExecutor.defaultDBErrorHandlePolicy(
                    ImmutableSet.of(
                        NoSuchTransaction,
                        ExceededTimeLimit,
                        WriteConflict,
                        LockFailed,
                        TransactionExceededLifetimeLimitSeconds,
                        DuplicateKey)),
                null,
                t -> !t.txnOk && t.retryable && !t.thrownError && !t.parkThread,
                timed,
                timed ? (waitTimeNanos - (nanoTime() - s)) : -1L,
                NANOSECONDS);

        if (outbound.txnOk) {
          // 尝试唤起下一个进程
          unparkSuccessor(false);
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

      BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
          (session, coll) -> {
            DeleteResult deleteResult =
                coll.deleteOne(
                    and(
                        eq("_id", this.getKey()),
                        eq("m", WRITE_MODE),
                        type("o", BsonType.ARRAY),
                        size("o", 1),
                        elemMatch("o", mappedHolder2AndFilter(holder))));

            return deleteResult.getDeletedCount() == 1
                ? ok()
                : thrownAnError("The current instance does not hold a write lock.");
          };
      TxnResponse txnResponse =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                  ImmutableSet.of(
                      NoSuchTransaction,
                      ExceededTimeLimit,
                      WriteConflict,
                      LockFailed,
                      TransactionExceededLifetimeLimitSeconds)),
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

  @AutoService(DistributeReadLock.class)
  class DistributeReadLockImp extends DistributeMongoSignalBase implements DistributeReadLock {

    public DistributeReadLockImp(
        Lease lease, String key, MongoClient mongoClient, MongoDatabase db) {
      super(lease, key, mongoClient, db, READ_WRITE_LOCK_NAMED);
    }

    @Override
    public boolean tryLock(Long waitTime, TimeUnit timeUnit) throws InterruptedException {
      Document holder = currHolder();
      BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
          (session, coll) -> {
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
              //lock.lock();
              return parkThread();
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
        boolean elapsed = parkCurrentThread("r", timed, s, waitTimeNanos);
        if (!elapsed) return false;

        // 循环检查状态
        checkState();

        TxnResponse txnResponse =
            commandExecutor.loopExecute(
                command,
                commandExecutor.defaultDBErrorHandlePolicy(
                    ImmutableSet.of(
                        NoSuchTransaction,
                        ExceededTimeLimit,
                        WriteConflict,
                        LockFailed,
                        TransactionExceededLifetimeLimitSeconds,
                        DuplicateKey)),
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

      BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
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

      TxnResponse outbound =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                  ImmutableSet.of(
                      NoSuchTransaction,
                      ExceededTimeLimit,
                      WriteConflict,
                      LockFailed,
                      TransactionExceededLifetimeLimitSeconds)),
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

  /**
   * 此方法不再需要再次Lock，事务内部已经进行了Lock
   *
   * @param expectedState 期望的锁的状态
   * @param timed 是否开启超时限制
   * @param s 开始事件
   * @param waitTimeNanos 等待的时间
   * @return 如果等待超时 返回true 否则false
   * @throws InterruptedException 如果主线程被shutdown，当前等待的线程会触发此错误
   */
  private boolean parkCurrentThread(String expectedState, boolean timed, long s, long waitTimeNanos)
      throws InterruptedException {
    lock.lock();
    try {
      // 不满足等待的条件 触发线程阻塞
      while (!(availableState == null
          || availableState.isEmpty()
          || availableState.equals(expectedState))) {

        if (timed) {
          return available.await((waitTimeNanos - (nanoTime() - s)), NANOSECONDS);
        } else {
          available.await();
          return true;
        }
      }
      return true;
    } finally {
      lock.unlock();
    }
  }

  private void forceUnlock() {
    BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
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

    TxnResponse txnResponse =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(
                ImmutableSet.of(
                    NoSuchTransaction,
                    ExceededTimeLimit,
                    WriteConflict,
                    LockFailed,
                    TransactionExceededLifetimeLimitSeconds)),
            null,
            t -> !t.txnOk && t.retryable);

    if (!txnResponse.txnOk) throw new SignalException("Unknown Error.");
  }

  /**
   * 满足条件会唤醒头部线程 并且将{@link DistributeWriteLockImp#availableState} 设置为空或者"r"
   *
   * @param event 删除事件或者锁的修改事件
   */
  @Subscribe
  void awake(ChangeStreamEvents.ReadWriteLockChangeAndRemovedEvent event) {
    Document fullDocument = event.fullDocument();
    List<Document> holders;
    // 当fullDocument为空时，对应的锁数据被删除的操作
    if (fullDocument == null
        || ((holders = fullDocument.getList("o", Document.class)) == null || holders.isEmpty())) {
      // 将锁的可用状态置空 代表可以执行读锁尝试 也可以执行写锁尝试
      availableState = "";
      unparkSuccessor(false);
      return;
    }

    String mode = fullDocument.getString("m");
    if (READ_MODE.equals(mode) && !holders.isEmpty()) {
      // 在写锁模式下，将锁的可用状态设置为r，代表读锁线程可以尝试着获取锁资源
      availableState = "r";
      unparkSuccessor(false);
    }
  }
}
