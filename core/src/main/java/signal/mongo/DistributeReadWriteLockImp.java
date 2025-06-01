package signal.mongo;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Filters.type;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.pull;
import static com.mongodb.client.model.Updates.set;
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
import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 *   "_id": "Test",
 *   "mode": "READ",
 *   "owners": [
 *     {
 *       "hostname": "",
 *       "lease": "29085153494899000",
 *       "thread": "ForkJoinPool.commonPool-worker-1-24",
 *       "enter_count": 1
 *     },
 *     {
 *       "hostname": "",
 *       "lease": "29085153494899000",
 *       "thread": "ForkJoinPool.commonPool-worker-2-25",
 *       "enter_count": 1
 *     }
 *   ],
 *   "version": 2
 * }
 *
 * }</pre>
 *
 * <p>锁的公平性：非公平锁
 */
@AutoService({DistributeReadWriteLock.class})
public final class DistributeReadWriteLockImp extends DistributeMongoSignalBase<RWLockDocument>
    implements DistributeReadWriteLock {

  private final Logger log = LoggerFactory.getLogger("signal.core.DistributeReadWriteLock");

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

  private final DistributeReadLockImp readLock;
  private final DistributeWriteLockImp writeLock;

  /** 当前锁的可用状态标识 */
  @Keep
  @GuardedBy("varHandle")
  @VisibleForTesting
  private StatefulVar<LockStateObject> state;

  private final VarHandle varHandle;

  private final EventBus eventBus;

  private final ReentrantLock lock;
  private final Condition writeLockAvailable;
  private final Condition readLockAvailable;

  DistributeReadWriteLockImp(
      Lease lease, String key, MongoClient mongoClient, MongoDatabase db, EventBus eventBus) {
    super(lease, key, mongoClient, db, READ_WRITE_LOCK_NAMED);

    this.state = new StatefulVar<>(null);

    try {
      varHandle =
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
    this.readLock = new DistributeReadLockImp(lease, key, mongoClient, db);
    this.writeLock = new DistributeWriteLockImp(lease, key, mongoClient, db);
  }

  @Override
  public DistributeReadLock readLock() {
    return readLock;
  }

  @Override
  public DistributeWriteLock writeLock() {
    return writeLock;
  }

  @CanIgnoreReturnValue
  private boolean safeUpdateState(
      StatefulVar<LockStateObject> captureState, LockStateObject newObj) {
    return varHandle.compareAndExchangeRelease(
            DistributeReadWriteLockImp.this, captureState, new StatefulVar<>(newObj))
        == captureState;
  }

  @Override
  protected void doClose() {
    eventBus.unregister(this);
    readLock.close();
    writeLock.close();
    unparkSuccessor(lock, writeLockAvailable, true);
    unparkSuccessor(lock, readLockAvailable, true);
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
      boolean cas,
      StatefulVar<LockStateObject> captureState,
      LockStateObject newStateValue) {}

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
          lockObj ->
              lockObj != null
                  && !lockObj.ownerHashCodes().isEmpty()
                  && !lockObj.ownerHashCodes().contains(thisOwner.hashCode()),
          lock -> (!lock.owners().isEmpty() && !lock.owners().contains(thisOwner)),
          lock -> lock.mode() == WRITE && lock.owners().contains(thisOwner),
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
      return execIsLockedTxnCommand(WRITE);
    }

    @Override
    public boolean isHeldByCurrentThread() {
      return execIsHeldByCurrentThreadTxnCommand(WRITE);
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
      StatefulVar<LockStateObject> captureState =
          (StatefulVar<LockStateObject>) varHandle.getAcquire(this);

      RWLockDocument rw = coll.find(session, eq("_id", this.getKey())).first();
      if (rw == null) {
        rw = new RWLockDocument(key, mode, List.of(thisOwner), 1L);
        InsertOneResult insertOneResult = coll.insertOne(session, rw);
        boolean success =
            insertOneResult.getInsertedId() != null && insertOneResult.wasAcknowledged();
        return new TryLockTxnResponse(
            success,
            !success,
            captureState,
            new LockStateObject(mode, Set.of(thisOwner.hashCode())));
      }

      if (nonRetryablePredicate.test(rw)) {
        return new TryLockTxnResponse(
            false,
            false,
            captureState,
            new LockStateObject(
                rw.mode(),
                rw.owners().stream()
                    .map(RWLockOwnerDocument::hashCode)
                    .collect(toUnmodifiableSet())));
      }

      long version = rw.version(), newVersion = version + 1L;

      List<Bson> condList = new ArrayList<>(8);
      condList.add(eq("_id", key));
      condList.add(eq("version", version));

      List<Bson> updateList = new ArrayList<>(8);
      updateList.add(inc("version", 1L));

      if (reenterPredicate.test(rw)) {
        condList.add(eq("owners.hostname", thisOwner.hostname()));
        condList.add(eq("owners.lease", thisOwner.lease()));
        condList.add(eq("owners.thread", thisOwner.thread()));
        updateList.add(inc("owners.$.enter_count", 1));
      } else {
        updateList.add(set("mode", mode));
        updateList.add(addToSet("owners", thisOwner));
      }
      boolean success =
          (rw = coll.findOneAndUpdate(session, and(condList), combine(updateList), UPDATE_OPTIONS))
                  != null
              && rw.version() == newVersion
              && rw.owners().contains(thisOwner);

      log.info("try lock update success {} ", success);

      return new TryLockTxnResponse(
          success,
          !success,
          captureState,
          success
              ? new LockStateObject(
                  rw.mode(),
                  rw.owners().stream()
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
                        (varHandle.getAcquire(DistributeReadWriteLockImp.this)));
                return parkPredicate.test(sv.value);
              },
              timed,
              startNanos,
              (waitTimeNanos - (nanoTime() - startNanos)));
      if (!elapsed) {
        log.warn("Timeout.");
        return false;
      }

      checkState();

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

      safeUpdateState(rsp.captureState, rsp.newObj);
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
          lockObj ->
              lockObj != null && (lockObj.mode == WRITE && !lockObj.ownerHashCodes.isEmpty()),
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
      return execIsLockedTxnCommand(READ);
    }

    @Override
    public boolean isHeldByCurrentThread() {
      return execIsHeldByCurrentThreadTxnCommand(READ);
    }

    @Override
    protected void doClose() {}

    @Override
    public Collection<?> getParticipants() {
      BiFunction<ClientSession, MongoCollection<RWLockDocument>, Collection<?>> command =
          (session, coll) -> {
            List<RWLockOwnerDocument> owners = new ArrayList<>(4);
            coll.aggregate(
                    session,
                    List.of(
                        match(
                            and(eq("_id", key), eq("mode", READ), type("owners", BsonType.ARRAY))),
                        unwind("$owners"),
                        project(
                            fields(
                                computed("hostname", "$owners.hostname"),
                                computed("lease", "$owners.lease"),
                                computed("thread", "$owners.thread"),
                                computed("enter_count", "$owners.enter_count")))),
                    RWLockOwnerDocument.class)
                .into(owners);
            return owners;
          };
      return commandExecutor.loopExecute(command);
    }
  }

  private boolean execIsLockedTxnCommand(RWLockMode mode) {
    // $mode == mode && $owners.length() > 0
    BiFunction<ClientSession, MongoCollection<RWLockDocument>, Boolean> command =
        (session, coll) ->
            coll.countDocuments(
                    session,
                    and(
                        eq("_id", key),
                        eq("mode", mode),
                        type("owners", BsonType.ARRAY),
                        expr(new Document("$gt", List.of(new Document("$size", "$owners"), 0)))))
                > 0;
    return commandExecutor.loopExecute(command);
  }

  private boolean execIsHeldByCurrentThreadTxnCommand(RWLockMode mode) {
    RWLockOwnerDocument thisOwner = buildCurrentOwner();
    BiFunction<ClientSession, MongoCollection<RWLockDocument>, Boolean> command =
        (session, coll) ->
            coll.countDocuments(
                    session,
                    and(
                        eq("_id", key),
                        eq("mode", mode),
                        type("owners", BsonType.ARRAY),
                        eq("owners.hostname", thisOwner.hostname()),
                        eq("owners.lease", thisOwner.lease()),
                        eq("owners.thread", thisOwner.thread())))
                > 0;
    return commandExecutor.loopExecute(command);
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

      if (rsp.cas) safeUpdateState(rsp.captureState, rsp.newStateValue);

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
      StatefulVar<LockStateObject> currState =
          (StatefulVar<LockStateObject>) varHandle.getAcquire(this);

      RWLockDocument rw = coll.find(session, eq("_id", key)).first();
      RWLockOwnerDocument thatOwner =
          Optional.ofNullable(rw)
              .map(RWLockDocument::owners)
              .flatMap(t -> t.stream().filter(thisOwner::equals).findFirst())
              .orElse(null);

      if (rw == null || thatOwner == null) {
        return new UnlockTxnResponse(
            false,
            false,
            String.format("The current instance does not hold a %s lock.", mode),
            true,
            currState,
            (rw == null || rw.owners() == null)
                ? null
                : new LockStateObject(
                    rw.mode(),
                    rw.owners().stream()
                        .map(RWLockOwnerDocument::hashCode)
                        .collect(toUnmodifiableSet())));
      }

      long version = rw.version(), newVersion = version + 1;
      Bson filter = and(eq("_id", key), eq("version", version));

      // 达到删除的条件
      if (rw.owners().size() == 1 && thatOwner.enterCount() <= 1) {
        DeleteResult deleteResult = coll.deleteOne(session, filter);
        boolean success = deleteResult.wasAcknowledged() && deleteResult.getDeletedCount() == 1L;
        return new UnlockTxnResponse(success, !success, null, success, currState, null);
      }

      List<Bson> filterList = new ArrayList<>(8);
      List<Bson> updateList = new ArrayList<>(8);
      filterList.add(eq("_id", key));
      filterList.add(eq("version", version));
      updateList.add(inc("version", 1L));

      if (thatOwner.enterCount() <= 1) {
        updateList.add(
            pull(
                "owners",
                new Document("hostname", thatOwner.hostname())
                    .append("lease", thatOwner.lease())
                    .append("thread", thatOwner.thread())));
      } else {
        filterList.add(eq("owners.hostname", thisOwner.hostname()));
        filterList.add(eq("owners.lease", thisOwner.lease()));
        filterList.add(eq("owners.thread", thisOwner.thread()));
        updateList.add(inc("owners.$.enter_count", -1));
      }

      boolean success =
          (rw =
                      coll.findOneAndUpdate(
                          session, and(filterList), combine(updateList), UPDATE_OPTIONS))
                  != null
              && rw.version() == newVersion;
      return new UnlockTxnResponse(
          success,
          !success,
          null,
          success,
          currState,
          rw == null
              ? null
              : new LockStateObject(
                  rw.mode(),
                  rw.owners().stream()
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
  }

  @DoNotCall
  @Subscribe
  void awakeSuccessor(ChangeEvents.RWLockChangeEvent event) {
    Document fullDocument = event.fullDocument();
    List<Document> owners =
        Optional.ofNullable(fullDocument)
            .map(t -> t.getList("owners", Document.class))
            .orElse(Collections.emptyList());

    lock.lock();
    try {
      Next:
      for (; ; ) {
        // 执行之前先获取state的值，避免和lock函数并发更新state的值冲突
        @SuppressWarnings("unchecked")
        StatefulVar<LockStateObject> currLockState =
            (StatefulVar<LockStateObject>) varHandle.getAcquire(DistributeReadWriteLockImp.this);

        // 当fullDocument为空时，对应的锁数据被删除的操作
        if (fullDocument == null || owners.isEmpty()) {

          if (safeUpdateState(currLockState, null)) {
            // 将锁的可用状态置空 代表可以执行读锁尝试 也可以执行写锁尝试
            if (lock.hasWaiters(writeLockAvailable)) writeLockAvailable.signal();
            if (lock.hasWaiters(readLockAvailable)) readLockAvailable.signal();
            log.info("awake successor safeUpdateState 2 null success. ");
            return;
          }
          Thread.onSpinWait();
          continue Next;
        }

        // ownerHashCodeList有值
        // TODO 需要考虑enter_count <= 0的情况吗？Why？
        Set<Integer> ownerHashCodeList =
            owners.stream()
                .map(
                    doc ->
                        new RWLockOwnerDocument(
                            doc.getString("hostname"),
                            doc.getString("lease"),
                            doc.getString("thread"),
                            doc.getInteger("enter_count")))
                .map(RWLockOwnerDocument::hashCode)
                .collect(toUnmodifiableSet());

        RWLockMode mode = RWLockMode.valueOf(fullDocument.getString("mode"));
        if (safeUpdateState(currLockState, new LockStateObject(mode, ownerHashCodeList))) {
          if (mode == READ) readLockAvailable.signal();
          return;
        }
        Thread.onSpinWait();
        continue Next;
      }
    } finally {
      lock.unlock();
    }
  }
}
