package signal.mongo;

import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static java.lang.System.nanoTime;
import static java.lang.Thread.interrupted;
import static java.util.Collections.singletonList;
import static signal.mongo.MongoErrorCode.NO_SUCH_TRANSACTION;
import static signal.mongo.MongoErrorCode.WRITE_CONFLICT;
import static signal.mongo.Utils.getCurrentHostname;
import static signal.mongo.Utils.getCurrentThreadName;

import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.ThreadSafe;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UnwindOptions;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import org.bson.Document;
import org.bson.conversions.Bson;
import signal.api.DistributeCondition;
import signal.api.DistributeLock;
import signal.api.Lease;
import signal.mongo.pojo.ConditionDocument;
import signal.mongo.pojo.ConditionWaiterDocument;

@ThreadSafe
public abstract class DistributeExclusiveBase<Doc> extends DistributeMongoSignalBase<Doc>
    implements DistributeLock {

  private static class InternalCondition {
    final Condition internalCondition;
    Boolean blocked;

    public InternalCondition(Condition internalCondition, Boolean blocked) {
      this.internalCondition = internalCondition;
      this.blocked = blocked;
    }
  }

  private final ReentrantLock internalLock;

  private final Map<String, InternalCondition> internalConditionList;

  public DistributeExclusiveBase(
      Lease lease, String key, MongoClient mongoClient, MongoDatabase db, String collectionName) {
    super(lease, key, mongoClient, db, collectionName);
    this.internalLock = new ReentrantLock();
    this.internalConditionList = new HashMap<>();
  }

  abstract Doc mappedDocument2Lock(Document document);

  abstract Long getVersion(Doc doc);

  abstract boolean isExclusiveByCurrentThread(ConditionWaiterDocument waiter);

  abstract boolean execUnlockCmdInTxn(ClientSession session, @NonNull Doc lock);

  abstract boolean execLockCmdInTxn(ClientSession session, @NonNull Doc lock);

  private record ConditionWaitTxnResponse(
      boolean success, boolean retryable, RuntimeException error) {}

  private record ConditionSignalTxnResponse(
      boolean success, boolean retryable, RuntimeException error) {}

  private record LockAndCondition(Object lock, ConditionDocument condition) {}

  /**
   * @see java.util.concurrent.locks.ReentrantLock#tryLock(long, TimeUnit)
   * @see ReentrantLock#lock()
   * @see ReentrantLock#unlock()
   * @see ReentrantLock#lockInterruptibly()
   * @see java.util.concurrent.Semaphore#tryAcquire(long, TimeUnit)
   * @see Semaphore#release()
   * @see java.util.concurrent.locks.Condition
   *     <p>{@code DistributeMutexLock lock = ... DistributeCondition condition =
   *     lock.newCondition(); <p>lock.lock(); condition.await(); lock.unlock();
   *     <p>DistributeMutexLock lock2 = ... DistributeCondition condition2 = lock2.newCondition();
   *     <p>lock2.lock(); condition2.await(); lock2.unlock(); }
   */
  protected class DistributeConditionObjectBase implements DistributeCondition {
    private final String conditionKey;
    private final MongoCollection<ConditionDocument> coll;
    private final CommandExecutor<ConditionDocument> conditionCmdExecutor;

    static final int CANCELLED = 0x80000000; // must be negative
    static final int COND = 2; // in a condition wait

    DistributeConditionObjectBase(String conditionKey, String conditionCollName) {
      this.conditionKey = conditionKey;
      this.coll = db.getCollection(conditionCollName, ConditionDocument.class);
      this.conditionCmdExecutor = new CommandExecutor<>(this, mongoClient, coll);
    }

    @Override
    public String conditionKey() {
      return conditionKey;
    }

    /**
     * @see java.util.concurrent.locks.Condition#await(long, TimeUnit)
     */
    private void enableWait() {
      if (!isHeldByCurrentThread()) {
        throw new IllegalMonitorStateException();
      }
    }

    private List<ConditionWaiterDocument> mappedDocuments2Waiters(List<Document> documents) {
      return documents.stream()
          .map(
              doc ->
                  new ConditionWaiterDocument(
                      doc.getString("hostname"),
                      doc.getString("lease"),
                      doc.getString("thread"),
                      doc.getInteger("status")))
          .toList();
    }

    // 从MongoDB8.0开始，$lookup可以在事务中进行
    private @Nullable LockAndCondition getConditionV8(ClientSession session) {
      List<Document> documents = new ArrayList<>(1);
      collection
          .aggregate(
              session,
              List.of(
                  match(eq("_id", key)),
                  lookup(coll.getNamespace().getCollectionName(), "_id", "lock_key", "condition"),
                  limit(1),
                  unwind("$conditions", new UnwindOptions().preserveNullAndEmptyArrays(true))),
              Document.class)
          .into(documents);
      if (documents.isEmpty()) return null;

      Document document = documents.get(0);

      Doc lock = mappedDocument2Lock(document);
      if (lock == null) return null;
      Document conditionDocument = document.get("condition", Document.class);

      return conditionDocument == null
          ? new LockAndCondition(lock, null)
          : new LockAndCondition(
              lock,
              new ConditionDocument(
                  conditionKey,
                  key,
                  mappedDocuments2Waiters(conditionDocument.getList("waiters", Document.class)),
                  conditionDocument.getLong("version"),
                  conditionDocument.getLong("timestamp")));
    }

    private ConditionWaiterDocument buildThisWaiter(int status) {
      return new ConditionWaiterDocument(
          getCurrentHostname(), lease.getId(), getCurrentThreadName(), status);
    }

    @SuppressWarnings("unchecked")
    private BiFunction<
            ClientSession, MongoCollection<ConditionDocument>, ConditionSignalTxnResponse>
        buildSignalTxnCommand(boolean all) {
      ConditionWaiterDocument thisWaiter = buildThisWaiter(0);
      return (session, coll) -> {
        LockAndCondition lockAndCondition = getConditionV8(session);

        Doc lock;
        if (lockAndCondition == null
            || (lock = (Doc) lockAndCondition.lock) == null
            || !isExclusiveByCurrentThread(thisWaiter))
          return new ConditionSignalTxnResponse(false, false, new IllegalMonitorStateException());

        ConditionDocument conditionDocument = lockAndCondition.condition;
        if (conditionDocument == null
            || conditionDocument.waiters() == null
            || conditionDocument.waiters().isEmpty()) {
          return new ConditionSignalTxnResponse(false, false, new IllegalMonitorStateException());
        }

      };
    }

    @SuppressWarnings("unchecked")
    private BiFunction<ClientSession, MongoCollection<ConditionDocument>, ConditionWaitTxnResponse>
        buildWaitTxnCommand() {
      ConditionWaiterDocument thisWaiter = buildThisWaiter(0);
      return (session, coll) -> {
        LockAndCondition lockAndCondition = getConditionV8(session);
        Doc lock;
        if (lockAndCondition == null
            || (lock = (Doc) lockAndCondition.lock) == null
            || !isExclusiveByCurrentThread(thisWaiter))
          return new ConditionWaitTxnResponse(false, false, new IllegalMonitorStateException());

        ConditionDocument conditionDocument = lockAndCondition.condition;
        if (conditionDocument == null) {
          conditionDocument =
              new ConditionDocument(
                  conditionKey, key, singletonList(buildThisWaiter(COND)), 1L, nanoTime());
          InsertOneResult insertOneResult = coll.insertOne(session, conditionDocument);
          if (insertOneResult.wasAcknowledged() || insertOneResult.getInsertedId() == null) {
            return new ConditionWaitTxnResponse(false, true, null);
          }
          boolean success = execUnlockCmdInTxn(session, lock);
          return new ConditionWaitTxnResponse(success, !success, null);
        }

        long version = conditionDocument.version(), newVersion = version + 1L;

        List<Bson> updateList = new ArrayList<>(2);
        updateList.add(inc("version", 1L));

        if (conditionDocument.waiters().contains(thisWaiter)) {
          updateList.add(addToSet("waiters", buildThisWaiter(COND)));
        }

        conditionDocument =
            coll.findOneAndUpdate(
                session,
                and(eq("_id", conditionKey), eq("version", version)),
                combine(updateList),
                UPDATE_OPTIONS);
        if (conditionDocument == null || conditionDocument.version() != newVersion)
          return new ConditionWaitTxnResponse(false, true, null);
        boolean success = execUnlockCmdInTxn(session, lock);
        return new ConditionWaitTxnResponse(success, !success, null);
      };
    }

    @Override
    public void await() throws InterruptedException {
      if (interrupted()) throw new InterruptedException();
      var command = buildWaitTxnCommand();

      for (; ; ) {
        ConditionWaitTxnResponse rsp =
            conditionCmdExecutor.loopExecute(
                command,
                commandExecutor.defaultDBErrorHandlePolicy(WRITE_CONFLICT, NO_SUCH_TRANSACTION),
                null,
                t -> !t.success && t.retryable && !interrupted());
        if (rsp.error != null) throw rsp.error;
        if (interrupted()) throw new InterruptedException();

        // spurious wakeup
        if (rsp.success) {
          internalLock.lock();
          try {
            InternalCondition internalCondition =
                internalConditionList.computeIfAbsent(
                    conditionKey,
                    _unused -> new InternalCondition(internalLock.newCondition(), true));
            internalCondition.blocked = true;
            while (internalCondition.blocked) {
              internalCondition.internalCondition.await();
            }
          } finally {
            internalLock.unlock();
            lock();
          }
          break;
        }
        Thread.onSpinWait();
      }
    }

    @Override
    public void awaitUninterruptibly() {
      var command = buildWaitTxnCommand();

      for (; ; ) {
        ConditionWaitTxnResponse rsp =
            conditionCmdExecutor.loopExecute(
                command,
                commandExecutor.defaultDBErrorHandlePolicy(WRITE_CONFLICT, NO_SUCH_TRANSACTION),
                null,
                t -> !t.success && t.retryable);
        if (rsp.error != null) throw rsp.error;

        // spurious wakeup
        if (rsp.success) {
          internalLock.lock();
          try {
            InternalCondition internalCondition =
                internalConditionList.computeIfAbsent(
                    conditionKey,
                    _unused -> new InternalCondition(internalLock.newCondition(), true));
            internalCondition.blocked = true;
            while (internalCondition.blocked) {
              internalCondition.internalCondition.awaitUninterruptibly();
            }
          } finally {
            internalLock.unlock();
            lock();
          }
          break;
        }
        Thread.onSpinWait();
      }
    }

    @CheckReturnValue
    @Override
    public long awaitNanos(long nanosTimeout) throws InterruptedException {
      if (interrupted()) throw new InterruptedException();

      long nanos = Math.max(nanosTimeout, 0L);
      long deadline = nanoTime() + nanos;
      AtomicLong atomicNanos = new AtomicLong(nanos);

      var command = buildWaitTxnCommand();
      for (; ; ) {

        if ((deadline - nanoTime()) <= 0L) return 0L;

        var rsp =
            conditionCmdExecutor.loopExecute(
                command,
                commandExecutor.defaultDBErrorHandlePolicy(WRITE_CONFLICT, NO_SUCH_TRANSACTION),
                null,
                t -> {
                  atomicNanos.set(deadline - nanoTime());
                  return !t.success && t.retryable && !interrupted() && atomicNanos.get() > 0L;
                });

        if (rsp.error != null) throw rsp.error;
        if (interrupted()) throw new InterruptedException();

        // spurious wakeup
        if (rsp.success) {
          if (atomicNanos.get() <= 0L) atomicNanos.set(0L);
          internalLock.lock();
          InternalCondition internalCondition =
              internalConditionList.computeIfAbsent(
                  conditionKey,
                  _unused -> new InternalCondition(internalLock.newCondition(), true));
          internalCondition.blocked = true;
          try {
            while (internalCondition.blocked) {
              long remainNanos = internalCondition.internalCondition.awaitNanos(atomicNanos.get());
              if (remainNanos <= 0L) return 0L;
            }
          } finally {
            internalCondition.blocked = false;
            internalLock.unlock();
            lock();
          }
          break;
        }
        Thread.onSpinWait();
      }
      long remaining = deadline - System.nanoTime(); // avoid overflow
      return (remaining <= atomicNanos.get()) ? remaining : Long.MIN_VALUE;
    }

    @Override
    public boolean await(long time, TimeUnit unit) throws InterruptedException {
      return false;
    }

    @Override
    public boolean awaitUntil(Date deadline) throws InterruptedException {
      return false;
    }

    @Override
    public void signal() {}

    @Override
    public void signalAll() {}
  }
}
