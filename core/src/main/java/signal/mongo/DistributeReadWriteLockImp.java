package signal.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.pull;
import static com.mongodb.client.model.Updates.set;
import static java.lang.System.identityHashCode;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static signal.mongo.CollectionNamed.READ_WRITE_LOCK_NAMED;
import static signal.mongo.MongoErrorCode.DUPLICATE_KEY;
import static signal.mongo.MongoErrorCode.NO_SUCH_TRANSACTION;
import static signal.mongo.MongoErrorCode.WRITE_CONFLICT;
import static signal.mongo.Utils.getCurrentHostname;
import static signal.mongo.Utils.getCurrentThreadName;
import static signal.mongo.Utils.parkCurrentThreadUntil;
import static signal.mongo.Utils.unparkSuccessor;
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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Predicate;
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

  private record LockStateObject(RWLockMode mode, Set<Integer> ownerHashCodes) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LockStateObject that = (LockStateObject) o;
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
  final StatefulVar<LockStateObject> state;

  private final VarHandle modeVarHandle;

  private final EventBus eventBus;

  private final ReentrantLock lock;
  private final Condition writeLockAvailable;
  private final Condition readLockAvailable;

  DistributeReadWriteLockImp(
      Lease lease, String key, MongoClient mongoClient, MongoDatabase db, EventBus eventBus) {
    super(lease, key, mongoClient, db, READ_WRITE_LOCK_NAMED);

    this.state = new StatefulVar<>(null);
    try {
      modeVarHandle =
          MethodHandles.lookup()
              .findVarHandle(DistributeReadWriteLockImp.class, "state", StatefulVar.class);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException(e);
    }

    this.eventBus = eventBus;
    this.lock = new ReentrantLock();
    this.writeLockAvailable = lock.newCondition();
    this.readLockAvailable = lock.newCondition();

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
      StatefulVar<LockStateObject> captureState, LockStateObject newObj) {
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
    readLockImp.close();
    writeLockImp.close();
    unparkSuccessor(lock, writeLockAvailable, true);
    forceUnlock();
  }

  private RWLockOwnerDocument buildCurrentOwner() {
    return new RWLockOwnerDocument(getCurrentHostname(), lease.getId(), getCurrentThreadName(), 1);
  }

  private record TryLockTxnResponse(
      boolean tryLockSuccess,
      boolean retryable,
      StatefulVar<LockStateObject> captureState,
      LockStateObject newObj) {}

  private record UnlockTxnResponse(
      boolean unlockSuccess,
      boolean retryable,
      String exceptionMessage,
      boolean casUpdateState,
      StatefulVar<LockStateObject> captureState,
      LockStateObject newObj) {}

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
      RWLockOwnerDocument thisOwner = buildCurrentOwner();
      long s = nanoTime(), waitTimeNanos = timeUnit.toNanos(waitTime);
      boolean timed = waitTime > 0;
      return execLockTxnCommand(
          thisOwner,
          WRITE,
          writeLockAvailable,
          lockObj -> {
            if (lockObj == null || lockObj.ownerHashCodes().isEmpty()) return false;
            return lockObj.mode != WRITE
                && !lockObj.ownerHashCodes().contains(thisOwner.hashCode());
          },
          lock -> (!lock.owners().isEmpty() && lock.mode() == WRITE),
          lock -> lock.mode() == READ && lock.owners().contains(thisOwner),
          s,
          waitTimeNanos,
          timed);
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
      buildLockTxnCommand(
          RWLockOwnerDocument thisOwner,
          RWLockMode mode,
          Predicate<RWLockDocument> nonRetryablePredicate,
          Predicate<RWLockDocument> reenterPredicate) {
    return (session, coll) -> {
      @SuppressWarnings("unchecked")
      StatefulVar<LockStateObject> state1 =
          (StatefulVar<LockStateObject>) modeVarHandle.getAcquire(this);

      RWLockDocument lock1 = coll.find(session, eq("_id", this.getKey())).first();
      if (lock1 == null) {
        lock1 = new RWLockDocument(key, mode, List.of(thisOwner), 1L);
        InsertOneResult insertOneResult = coll.insertOne(session, lock1);
        boolean success =
            insertOneResult.getInsertedId() != null && insertOneResult.wasAcknowledged();
        return new TryLockTxnResponse(
            success, !success, state1, new LockStateObject(mode, Set.of(thisOwner.hashCode())));
      }

      if (nonRetryablePredicate.test(lock1)) {
        return new TryLockTxnResponse(
            false,
            false,
            state1,
            new LockStateObject(
                lock1.mode(),
                lock1.owners().stream()
                    .map(RWLockOwnerDocument::hashCode)
                    .collect(toUnmodifiableSet())));
      }

      long version = lock1.version(), newVersion = version + 1L;

      List<Bson> condList = new ArrayList<>(8);
      condList.add(eq("_id", key));
      condList.add(eq("version", version));

      List<Bson> updateList = new ArrayList<>(8);
      updateList.add(inc("version", 1L));

      if (reenterPredicate.test(lock1)) {
        condList.add(eq("owners.hostname", thisOwner.hostname()));
        condList.add(eq("owners.lease", thisOwner.lease()));
        condList.add(eq("owners.thread", thisOwner.thread()));
        updateList.add(inc("owners.$.enter_count", 1));
      } else {
        updateList.add(set("mode", mode));
        updateList.add(addToSet("owners", thisOwner));
      }
      boolean success =
          (lock1 =
                      coll.findOneAndUpdate(
                          session, and(condList), combine(updateList), UPDATE_OPTIONS))
                  != null
              && lock1.version() == newVersion
              && lock1.owners().contains(thisOwner);
      return new TryLockTxnResponse(
          success,
          !success,
          state1,
          success
              ? new LockStateObject(
                  lock1.mode(),
                  lock1.owners().stream()
                      .map(RWLockOwnerDocument::hashCode)
                      .collect(toUnmodifiableSet()))
              : null);
    };
  }

  private boolean execLockTxnCommand(
      RWLockOwnerDocument thisOwner,
      RWLockMode mode,
      Condition available,
      Predicate<LockStateObject> parkPredicate,
      Predicate<RWLockDocument> nonRetryablePredicate,
      Predicate<RWLockDocument> reenterPredicate,
      long startNanos,
      long waitTimeNanos,
      boolean timed)
      throws InterruptedException {

    for (; ; ) {
      checkState();
      boolean elapsed =
          parkCurrentThreadUntil(
              lock,
              available,
              () -> {
                @SuppressWarnings("unchecked")
                StatefulVar<LockStateObject> sv =
                    ((StatefulVar<LockStateObject>)
                        (modeVarHandle.getAcquire(DistributeReadWriteLockImp.this)));
                return parkPredicate.test(sv.value);
              },
              timed,
              startNanos,
              (waitTimeNanos - (nanoTime() - startNanos)));

      if (!elapsed) {
        log.log(Level.INFO, "Timeout.");
        return false;
      }

      BiFunction<ClientSession, MongoCollection<RWLockDocument>, TryLockTxnResponse> command =
          this.buildLockTxnCommand(thisOwner, mode, nonRetryablePredicate, reenterPredicate);

      TryLockTxnResponse rsp =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                  NO_SUCH_TRANSACTION, WRITE_CONFLICT, DUPLICATE_KEY),
              null,
              t -> !t.tryLockSuccess && t.retryable,
              timed,
              timed ? (waitTimeNanos - (nanoTime() - startNanos)) : -1L,
              NANOSECONDS);

      safeUpdateModeIgnoreFailure(rsp.captureState, rsp.newObj);
      if (rsp.tryLockSuccess) {
        if (mode.equals(READ)) {
          unparkSuccessor(lock, available, false);
        }
        return true;
      }
      Thread.onSpinWait();
    }
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
      RWLockOwnerDocument thisOwner = buildCurrentOwner();
      long s = nanoTime(), waitTimeNanos = timeUnit.toNanos(waitTime);
      boolean timed = waitTime > 0;
      return execLockTxnCommand(
          thisOwner,
          READ,
          readLockAvailable,
          lockObj -> {
            if (lockObj == null) return false;
            return (lockObj.mode == WRITE && !lockObj.ownerHashCodes.isEmpty());
          },
          lock -> (!lock.owners().isEmpty() && lock.mode() == WRITE),
          lock -> lock.mode() == READ && lock.owners().contains(thisOwner),
          s,
          waitTimeNanos,
          timed);
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
    RWLockOwnerDocument thisOwner = buildCurrentOwner();
    return (session, coll) -> {
      @SuppressWarnings("unchecked")
      StatefulVar<LockStateObject> state1 =
          (StatefulVar<LockStateObject>) modeVarHandle.getAcquire(this);

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
                : new LockStateObject(
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
              : new LockStateObject(
                  lock.mode(),
                  lock.owners().stream()
                      .map(RWLockOwnerDocument::hashCode)
                      .collect(toUnmodifiableSet())));
    };
  }

  private void forceUnlock() {
    BiFunction<ClientSession, MongoCollection<RWLockDocument>, Boolean> command =
        (session, coll) -> {
          RWLockDocument lock = coll.find(session, eq("_id", this.getKey())).first();
          if (lock == null) return true;

          // 达到删除条件
          long version = lock.version(), newVersion = version + 1;
          var filter = and(eq("_id", this.getKey()), eq("version", version));
          if (lock.owners().isEmpty()) {
            DeleteResult deleteResult = coll.deleteOne(session, filter);
            return deleteResult.getDeletedCount() == 1L && deleteResult.wasAcknowledged();
          }

          // 将所有的Holder删除
          var update = pull("owners", new Document("lease", lease.getId()));
          return ((lock = coll.findOneAndUpdate(session, filter, update, UPDATE_OPTIONS)) != null
              && lock.version() == newVersion
              && (lock.owners().stream().noneMatch(t -> t.lease().equals(lease.getId()))));
        };

    Boolean forceUnlockRsp =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(
                NO_SUCH_TRANSACTION, WRITE_CONFLICT, DUPLICATE_KEY),
            null,
            t -> !t);

    log.log(Level.INFO, "forceUnlockRsp: {0}", new Object[] {forceUnlockRsp});
  }

  @DoNotCall
  @Subscribe
  void awakeSuccessor(ChangeEvents.RWLockChangeEvent event) {
    Document fullDocument = event.fullDocument();

    List<Document> owners =
        Optional.ofNullable(fullDocument)
            .map(t -> t.getList("owners", Document.class))
            .orElse(Collections.emptyList());

    Next:
    for (; ; ) {
      // 执行之前先获取state的值，避免和lock函数并发更新state的值冲突
      @SuppressWarnings("unchecked")
      StatefulVar<LockStateObject> currLockState =
          (StatefulVar<LockStateObject>) modeVarHandle.getAcquire(DistributeReadWriteLockImp.this);
      int currLockStateHashCode = identityHashCode(currLockState);

      // 当fullDocument为空时，对应的锁数据被删除的操作
      if (fullDocument == null || owners.isEmpty()) {
        if (currLockStateHashCode
            == identityHashCode(
                modeVarHandle.compareAndExchangeRelease(
                    DistributeReadWriteLockImp.this,
                    currLockState,
                    new StatefulVar<LockStateObject>(null)))) {
          // 将锁的可用状态置空 代表可以执行读锁尝试 也可以执行写锁尝试
          unparkSuccessor(lock, writeLockAvailable, false);
          unparkSuccessor(lock, readLockAvailable, false);
          return;
        }

        Thread.onSpinWait();
        continue Next;
      }

      RWLockMode mode = RWLockMode.valueOf(fullDocument.getString("mode"));

      Set<Integer> ownerHashCodeList =
          fullDocument.getList("owners", Document.class).stream()
              .map(
                  doc ->
                      new RWLockOwnerDocument(
                              doc.getString("hostname"),
                              doc.getString("lease"),
                              doc.getString("thread"),
                              doc.getInteger("enter_count"))
                          .hashCode())
              .collect(toUnmodifiableSet());

      if (currLockStateHashCode
          == identityHashCode(
              modeVarHandle.compareAndExchangeRelease(
                  DistributeReadWriteLockImp.this,
                  currLockState,
                  new StatefulVar<>(new LockStateObject(mode, ownerHashCodeList))))) {
        unparkSuccessor(lock, writeLockAvailable, false);
        return;
      }
      Thread.onSpinWait();
      continue Next;
    }
  }
}
