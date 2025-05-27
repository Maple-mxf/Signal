package signal.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static java.lang.System.identityHashCode;
import static java.lang.System.nanoTime;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static signal.mongo.CollectionNamed.READ_WRITE_LOCK_NAMED;
import static signal.mongo.MongoErrorCode.DUPLICATE_KEY;
import static signal.mongo.MongoErrorCode.NO_SUCH_TRANSACTION;
import static signal.mongo.MongoErrorCode.WRITE_CONFLICT;
import static signal.mongo.Utils.getCurrentHostname;
import static signal.mongo.Utils.getCurrentThreadName;
import static signal.mongo.Utils.parkCurrentThreadUntil;
import static signal.mongo.pojo.RWLockMode.READ;
import static signal.mongo.pojo.RWLockMode.WRITE;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;
import org.bson.conversions.Bson;
import signal.api.DistributeReadWriteLock;
import signal.api.Lease;
import signal.api.SignalException;
import signal.mongo.pojo.RWLockDocument;
import signal.mongo.pojo.RWLockMode;
import signal.mongo.pojo.RWLockOwnerDocument;

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
public final class DistributeReadWriteLockImp extends DistributeMongoSignalBase<RWLockDocument>
    implements DistributeReadWriteLock {

  private final Logger log = Logger.getLogger("DistributeReadWriteLock");

  private record LockOwnerObject(RWLockMode mode, Set<Integer> ownerHashCodes) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LockOwnerObject that = (LockOwnerObject) o;
      return mode == that.mode && Objects.equals(ownerHashCodes, that.ownerHashCodes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mode, ownerHashCodes);
    }
  }

  // TODO 锁的可重入性
  private final DistributeReadLockImp readLockImp;
  // TODO 锁的可重入性
  private final DistributeWriteLockImp writeLockImp;

  /** 当前锁的可用状态标识 */
  @Keep
  @GuardedBy("modeVarHandle")
  @VisibleForTesting
  final StatefulVar<LockOwnerObject> state;

  private final VarHandle modeVarHandle;

  private final EventBus eventBus;

  private final ReentrantLock lock;
  private final Condition available;

  DistributeReadWriteLockImp(
      Lease lease, String key, MongoClient mongoClient, MongoDatabase db, EventBus eventBus) {
    super(lease, key, mongoClient, db, READ_WRITE_LOCK_NAMED);

    this.state = new StatefulVar<>(null);
    try {
      modeVarHandle =
          MethodHandles.lookup()
              .findVarHandle(DistributeReadWriteLockImp.class, "mode", StatefulVar.class);
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

  private void safeUpdateModeIgnoreFailure(
      StatefulVar<LockOwnerObject> captureState, LockOwnerObject newObj) {
    Object oldObj =
        modeVarHandle.compareAndExchangeRelease(
            DistributeReadWriteLockImp.this, captureState, new StatefulVar<>(newObj));
    log.log(
        Level.INFO,
        "safeUpdateModeIgnoreFailure success ? {0}",
        new Object[] {identityHashCode(oldObj) == captureState.hashCode()});
  }

  @Override
  protected void doClose() {
    eventBus.unregister(this);
    unparkSuccessor(true);
    readLockImp.close();
    writeLockImp.close();
    forceUnlock();
  }

  private RWLockOwnerDocument buildCurrentOwner(int enterCount) {
    return new RWLockOwnerDocument(
        getCurrentHostname(), lease.getLeaseID(), getCurrentThreadName(), enterCount);
  }

  private record TryLockTxnResponse(
      boolean tryLockSuccess,
      boolean retryable,
      StatefulVar<LockOwnerObject> captureState,
      LockOwnerObject newObj) {}

  private record UnlockTxnResponse(
      boolean unlockSuccess,
      boolean retryable,
      String exceptionMessage,
      boolean casUpdateState,
      StatefulVar<LockOwnerObject> captureState,
      LockOwnerObject newObj) {}

  @ThreadSafe
  @AutoService(DistributeWriteLock.class)
  private class DistributeWriteLockImp extends DistributeMongoSignalBase<RWLockDocument>
      implements DistributeWriteLock {

    public DistributeWriteLockImp(
        Lease lease, String key, MongoClient mongoClient, MongoDatabase db) {
      super(lease, key, mongoClient, db, READ_WRITE_LOCK_NAMED);
    }

    @CheckReturnValue
    @Override
    public boolean tryLock(Long waitTime, TimeUnit timeUnit) throws InterruptedException {
      RWLockOwnerDocument thisOwner = buildCurrentOwner(1);
      BiFunction<ClientSession, MongoCollection<RWLockDocument>, TryLockTxnResponse> command =
          (session, coll) -> {
            @SuppressWarnings("unchecked")
            StatefulVar<LockOwnerObject> state =
                (StatefulVar<LockOwnerObject>)
                    modeVarHandle.getAcquire(DistributeReadWriteLockImp.this);

            RWLockDocument lock = coll.find(session, eq("_id", this.getKey())).first();
            if (lock == null) {
              lock = new RWLockDocument(key, WRITE, singletonList(thisOwner), 1L);
              InsertOneResult insertOneResult = coll.insertOne(session, lock);
              boolean success = insertOneResult.getInsertedId() != null;
              return new TryLockTxnResponse(
                  success,
                  !success,
                  state,
                  new LockOwnerObject(WRITE, Set.of(thisOwner.hashCode())));
            }
            if (!lock.owners().isEmpty() && !lock.owners().contains(thisOwner)) {
              return new TryLockTxnResponse(
                  false,
                  false,
                  state,
                  new LockOwnerObject(
                      lock.mode(),
                      lock.owners().stream().map(Object::hashCode).collect(toUnmodifiableSet())));
            }

            long version = lock.version(), newVersion = version + 1;

            List<Bson> condList = new ArrayList<>(8);
            condList.add(eq("_id", this.getKey()));
            condList.add(eq("version", version));

            List<Bson> updateList = new ArrayList<>(8);
            updateList.add(inc("version", 1L));

            if (lock.owners().contains(thisOwner)) {
              condList.add(eq("owners.hostname", thisOwner.hostname()));
              condList.add(eq("owners.lease", thisOwner.lease()));
              condList.add(eq("owners.thread", thisOwner.thread()));
              updateList.add(inc("owners.$.enter_count", 1));
            } else if (lock.owners().isEmpty()) {
              updateList.add(addToSet("owners", thisOwner));
              updateList.add(set("mode", WRITE));
            }
            boolean success =
                (lock =
                            coll.findOneAndUpdate(
                                session, and(condList), combine(updateList), UPDATE_OPTIONS))
                        != null
                    && lock.version() == newVersion
                    && lock.owners().contains(thisOwner);
            return new TryLockTxnResponse(
                success,
                !success,
                state,
                success ? new LockOwnerObject(lock.mode(), Set.of(thisOwner.hashCode())) : null);
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
                  StatefulVar<LockOwnerObject> sv =
                      (StatefulVar<LockOwnerObject>)
                          modeVarHandle.getAcquire(DistributeReadWriteLockImp.this);
                  if (sv.value == null || sv.value.ownerHashCodes().isEmpty()) return false;

                  return sv.value.mode != WRITE
                      || !sv.value.ownerHashCodes().contains(thisOwner.hashCode());
                },
                timed,
                s,
                (waitTimeNanos - (nanoTime() - s)));

        if (!elapsed) {
          log.log(Level.WARNING, "Timeout.");
          return false;
        }

        TryLockTxnResponse rsp =
            commandExecutor.loopExecute(
                command,
                commandExecutor.defaultDBErrorHandlePolicy(
                    NO_SUCH_TRANSACTION, WRITE_CONFLICT, DUPLICATE_KEY),
                null,
                t -> t.retryable && !t.tryLockSuccess,
                timed,
                timed ? (waitTimeNanos - (nanoTime() - s)) : -1L,
                NANOSECONDS);

        safeUpdateModeIgnoreFailure(rsp.captureState, rsp.newObj);
        if (rsp.tryLockSuccess) return true;
        Thread.onSpinWait();
      }
    }

    @Override
    public void unlock() {
      execUnlockTxnCommand(WRITE);
    }

    @Override
    public boolean isLocked() {
      return false;
    }

    @Override
    public boolean isHeldByCurrentThread() {
      return false;
    }

    @Override
    protected void doClose() {}

    @Override
    public Object getOwner() {
      return null;
    }
  }

  private BiFunction<ClientSession, MongoCollection<RWLockDocument>, TryLockTxnResponse>
      buildLockTxnCommand(RWLockMode mode) {
    RWLockOwnerDocument thisOwner = buildCurrentOwner(1);
    BiFunction<ClientSession, MongoCollection<RWLockDocument>, TryLockTxnResponse> command =
        (session, coll) -> {

          // 执行之前先获取state revision的值，避免和wakeHead函数并发更新state的值冲突
          @SuppressWarnings("unchecked")
          StatefulVar<LockOwnerObject> state =
              (StatefulVar<LockOwnerObject>)
                  modeVarHandle.getAcquire(DistributeReadWriteLockImp.this);

          RWLockDocument lock = coll.find(session, eq("_id", this.getKey())).first();
          if (lock == null) {
            lock = new RWLockDocument(key, mode, List.of(thisOwner), 1L);
            InsertOneResult insertOneResult = coll.insertOne(session, lock);
            boolean success =
                insertOneResult.getInsertedId() != null && insertOneResult.wasAcknowledged();
            return new TryLockTxnResponse(
                success, !success, state, new LockOwnerObject(mode, Set.of(thisOwner.hashCode())));
          }

          // 如果当前锁的状态是写锁状态，并且有线程占用(包含当前线程占用写锁的情况，1个线程不允许同时获取读锁和写锁)，则需要挂起当前线程

          if (!lock.owners().isEmpty() && lock.mode() == WRITE) {
            return new TryLockTxnResponse(
                false,
                false,
                state,
                new LockOwnerObject(
                    lock.mode(),
                    lock.owners().stream()
                        .map(RWLockOwnerDocument::hashCode)
                        .collect(toUnmodifiableSet())));
          }

          long version = lock.version(), newVersion = version + 1L;

          List<Bson> condList = new ArrayList<>(8);
          condList.add(eq("_id", key));
          condList.add(eq("version", version));

          List<Bson> updateList = new ArrayList<>(8);
          updateList.add(inc("version", 1L));

          if (lock.owners().isEmpty()) {
            updateList.add(set("mode", READ));
            updateList.add(addToSet("owners", thisOwner));
          } else if (lock.mode() == READ && lock.owners().contains(thisOwner)) {
            condList.add(eq("owners.hostname", thisOwner.hostname()));
            condList.add(eq("owners.lease", thisOwner.lease()));
            condList.add(eq("owners.thread", thisOwner.thread()));
            updateList.add(inc("owners.$.enter_count", 1));
          }
          boolean success =
              (lock =
                          coll.findOneAndUpdate(
                              session, and(condList), combine(updateList), UPDATE_OPTIONS))
                      != null
                  && lock.version() == newVersion
                  && lock.owners().contains(thisOwner);
          return new TryLockTxnResponse(
              success,
              !success,
              state,
              success
                  ? new LockOwnerObject(
                      lock.mode(),
                      lock.owners().stream()
                          .map(RWLockOwnerDocument::hashCode)
                          .collect(toUnmodifiableSet()))
                  : null);
        };
  }

  @ThreadSafe
  @AutoService(DistributeReadLock.class)
  class DistributeReadLockImp extends DistributeMongoSignalBase<RWLockDocument>
      implements DistributeReadLock {

    DistributeReadLockImp(Lease lease, String key, MongoClient mongoClient, MongoDatabase db) {
      super(lease, key, mongoClient, db, READ_WRITE_LOCK_NAMED);
    }

    @CheckReturnValue
    @Override
    public boolean tryLock(Long waitTime, TimeUnit timeUnit) throws InterruptedException {
      RWLockOwnerDocument thisOwner = buildCurrentOwner(1);
      BiFunction<ClientSession, MongoCollection<RWLockDocument>, TryLockTxnResponse> command =
          (session, coll) -> {

            // 执行之前先获取state revision的值，避免和wakeHead函数并发更新state的值冲突
            @SuppressWarnings("unchecked")
            StatefulVar<LockOwnerObject> state =
                (StatefulVar<LockOwnerObject>)
                    modeVarHandle.getAcquire(DistributeReadWriteLockImp.this);

            RWLockDocument lock = coll.find(session, eq("_id", this.getKey())).first();
            if (lock == null) {
              lock = new RWLockDocument(key, READ, List.of(thisOwner), 1L);
              InsertOneResult insertOneResult = coll.insertOne(session, lock);
              boolean success =
                  insertOneResult.getInsertedId() != null && insertOneResult.wasAcknowledged();
              return new TryLockTxnResponse(
                  success,
                  !success,
                  state,
                  new LockOwnerObject(READ, Set.of(thisOwner.hashCode())));
            }

            // 如果当前锁的状态是写锁状态，并且有线程占用(包含当前线程占用写锁的情况，1个线程不允许同时获取读锁和写锁)，则需要挂起当前线程
            if (!lock.owners().isEmpty() && lock.mode() == WRITE) {
              return new TryLockTxnResponse(
                  false,
                  false,
                  state,
                  new LockOwnerObject(
                      WRITE,
                      lock.owners().stream()
                          .map(RWLockOwnerDocument::hashCode)
                          .collect(toUnmodifiableSet())));
            }

            long version = lock.version(), newVersion = version + 1L;

            List<Bson> condList = new ArrayList<>(8);
            condList.add(eq("_id", key));
            condList.add(eq("version", version));

            List<Bson> updateList = new ArrayList<>(8);
            updateList.add(inc("version", 1L));

            if (lock.owners().isEmpty()) {
              updateList.add(set("mode", READ));
              updateList.add(addToSet("owners", thisOwner));
            } else if (lock.mode() == READ && lock.owners().contains(thisOwner)) {
              condList.add(eq("owners.hostname", thisOwner.hostname()));
              condList.add(eq("owners.lease", thisOwner.lease()));
              condList.add(eq("owners.thread", thisOwner.thread()));
              updateList.add(inc("owners.$.enter_count", 1));
            }
            boolean success =
                (lock =
                            coll.findOneAndUpdate(
                                session, and(condList), combine(updateList), UPDATE_OPTIONS))
                        != null
                    && lock.version() == newVersion
                    && lock.owners().contains(thisOwner);
            return new TryLockTxnResponse(
                success,
                !success,
                state,
                success
                    ? new LockOwnerObject(
                        lock.mode(),
                        lock.owners().stream()
                            .map(RWLockOwnerDocument::hashCode)
                            .collect(toUnmodifiableSet()))
                    : null);
          };

      long s = nanoTime(), waitTimeNanos = timeUnit.toNanos(waitTime);
      boolean timed = waitTime > 0;

      for (; ; ) {
        checkState();

        boolean elapsed =
            parkCurrentThreadUntil(
                lock,
                available,
                () -> {
                  @SuppressWarnings("unchecked")
                  StatefulVar<LockOwnerObject> sv =
                      ((StatefulVar<LockOwnerObject>)
                          (modeVarHandle.getAcquire(DistributeReadWriteLockImp.this)));
                  if (sv.value == null) return false;
                  return (sv.value.mode == WRITE && !sv.value.ownerHashCodes.isEmpty());
                },
                timed,
                s,
                (waitTimeNanos - (nanoTime() - s)));

        if (!elapsed) {
          log.log(Level.INFO, "Timeout.");
          return false;
        }

        TryLockTxnResponse rsp =
            commandExecutor.loopExecute(
                command,
                commandExecutor.defaultDBErrorHandlePolicy(
                    NO_SUCH_TRANSACTION, WRITE_CONFLICT, DUPLICATE_KEY),
                null,
                t -> !t.tryLockSuccess && t.retryable,
                timed,
                timed ? (waitTimeNanos - (nanoTime() - s)) : -1L,
                NANOSECONDS);

        safeUpdateModeIgnoreFailure(rsp.captureState, rsp.newObj);
        if (rsp.tryLockSuccess) return true;
        Thread.onSpinWait();
      }
    }

    @Override
    public void unlock() {
      execUnlockTxnCommand(READ);
    }

    @Override
    public boolean isLocked() {
      return false;
    }

    @Override
    public boolean isHeldByCurrentThread() {
      return false;
    }

    @Override
    protected void doClose() {}

    @Override
    public Collection<?> getParticipants() {
      return null;
    }
  }

  private void execUnlockTxnCommand(RWLockMode mode) {
    BiFunction<ClientSession, MongoCollection<RWLockDocument>, UnlockTxnResponse> command =
        buildUnlockTxnCommand(mode);
    Next:
    for (; ; ) {
      checkState();
      UnlockTxnResponse rsp =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                  NO_SUCH_TRANSACTION, WRITE_CONFLICT, DUPLICATE_KEY),
              null,
              t ->
                  !t.unlockSuccess
                      && t.retryable
                      && (t.exceptionMessage == null || t.exceptionMessage.isEmpty()));

      if (rsp.casUpdateState) safeUpdateModeIgnoreFailure(rsp.captureState, rsp.newObj);

      if (rsp.unlockSuccess) break Next;
      if (rsp.exceptionMessage != null && !rsp.exceptionMessage.isEmpty())
        throw new SignalException(rsp.exceptionMessage);
      Thread.onSpinWait();
    }
  }

  private BiFunction<ClientSession, MongoCollection<RWLockDocument>, UnlockTxnResponse>
      buildUnlockTxnCommand(RWLockMode mode) {
    RWLockOwnerDocument thisOwner = buildCurrentOwner(1);
    return (session, coll) -> {
      @SuppressWarnings("unchecked")
      StatefulVar<LockOwnerObject> state1 =
          (StatefulVar<LockOwnerObject>) modeVarHandle.getAcquire(this);

      RWLockDocument lock = coll.find(session, eq("_id", key)).first();
      if (lock == null || lock.mode() != mode || !lock.owners().contains(thisOwner)) {
        return new UnlockTxnResponse(
            false,
            false,
            String.format("The current instance does not hold a %s lock.", mode),
            true,
            state1,
            lock == null
                ? null
                : new LockOwnerObject(
                    lock.mode(),
                    lock.owners().stream()
                        .map(RWLockOwnerDocument::hashCode)
                        .collect(toUnmodifiableSet())));
      }

      long version = lock.version(), newVersion = version + 1;
      Bson filter = and(eq("_id", key), eq("version", version));

      RWLockOwnerDocument thatOwner = lock.owners().get(0);
      if (thatOwner.enterCount() <= 1) {
        DeleteResult deleteResult = coll.deleteOne(session, filter);
        boolean success = deleteResult.wasAcknowledged() && deleteResult.getDeletedCount() == 1L;
        return new UnlockTxnResponse(success, !success, null, success, state1, null);
      }
      lock =
          coll.findOneAndUpdate(
              session,
              and(
                  eq("_id", key),
                  eq("version", version),
                  eq("owners.hostname", thisOwner.hostname()),
                  eq("owners.lease", thisOwner.lease()),
                  eq("owners.thread", thisOwner.thread())),
              combine(inc("version", 1L), inc("owners.$.enter_count", -1)),
              UPDATE_OPTIONS);

      boolean success = lock != null && lock.version() == newVersion;
      return new UnlockTxnResponse(
          success,
          !success,
          null,
          success,
          state1,
          lock == null
              ? null
              : new LockOwnerObject(
                  lock.mode(),
                  lock.owners().stream()
                      .map(RWLockOwnerDocument::hashCode)
                      .collect(toUnmodifiableSet())));
    };
  }

  //  private void forceUnlock() {
  //    BiFunction<ClientSession, MongoCollection<Document>, CommonTxnResponse> command =
  //        (session, coll) -> {
  //          Document lock = coll.find(session, eq("_id", this.getKey())).first();
  //          if (lock == null) return ok();
  //
  //          List<Document> holders = lock.getList("o", Document.class);
  //
  //          // 达到删除条件
  //          long revision = lock.getLong("v"), newRevision = revision + 1;
  //          var filter = and(eq("_id", this.getKey()), eq("v", revision));
  //          if (holders == null || holders.isEmpty()) {
  //            DeleteResult deleteResult = coll.deleteOne(session, filter);
  //            return deleteResult.getDeletedCount() == 1L ? ok() : retryableError();
  //          }
  //
  //          // 将所有的Holder删除
  //          var update = pull("o", new Document("lease", this.getLease().getLeaseID()));
  //          return ((lock = coll.findOneAndUpdate(session, filter, update, UPDATE_OPTIONS)) !=
  // null
  //                  && lock.getLong("v") == newRevision
  //                  && ((holders = lock.getList("o", Document.class)) == null
  //                      || holders.isEmpty()
  //                      || holders.stream()
  //                          .noneMatch(
  //                              t -> t.getString("lease").equals(this.getLease().getLeaseID()))))
  //              ? ok()
  //              : retryableError();
  //        };
  //
  //    CommonTxnResponse txnResponse =
  //        commandExecutor.loopExecute(
  //            command,
  //            commandExecutor.defaultDBErrorHandlePolicy(
  //                NO_SUCH_TRANSACTION, WRITE_CONFLICT, LOCK_FAILED),
  //            null,
  //            t -> !t.txnOk && t.retryable);
  //
  //    if (!txnResponse.txnOk) throw new SignalException("Unknown Error.");
  //  }

  /**
   * 满足条件会唤醒头部线程 并且将{@link DistributeWriteLockImp#mode} 设置为空或者"r"
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
          (StatefulVar<String>) modeVarHandle.getAcquire(DistributeReadWriteLockImp.this);

      // 当fullDocument为空时，对应的锁数据被删除的操作
      if (fullDocument == null || holders == null || holders.isEmpty()) {

        if (identityHashCode(currState)
            == identityHashCode(
                modeVarHandle.compareAndExchangeRelease(
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
                modeVarHandle.compareAndExchangeRelease(
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
