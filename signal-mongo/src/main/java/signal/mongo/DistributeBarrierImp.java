package signal.mongo;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.type;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static java.util.Collections.emptySet;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;
import static signal.mongo.CollectionNamed.BARRIER_NAMED;
import static signal.mongo.MongoErrorCode.LockBusy;
import static signal.mongo.MongoErrorCode.LockFailed;
import static signal.mongo.MongoErrorCode.LockTimeout;
import static signal.mongo.MongoErrorCode.NoSuchTransaction;
import static signal.mongo.MongoErrorCode.WriteConflict;
import static signal.mongo.TxnResponse.ok;
import static signal.mongo.TxnResponse.parkThreadWithSuccess;
import static signal.mongo.TxnResponse.retryableError;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.errorprone.annotations.DoNotCall;
import com.google.errorprone.annotations.ThreadSafe;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import java.util.Collection;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import org.bson.BsonType;
import org.bson.Document;
import signal.api.DistributeBarrier;
import signal.api.Holder;
import signal.api.Lease;
import signal.api.SignalException;

/**
 * 数据存储格式
 *
 * <pre>{@code
 *  {
 *   _id: 'Test-Barrier',
 *   v: Long('7'),
 *   o: [
 *     {
 *       hostname: 'D0C058',
 *       thread: Long('24'),
 *       lease: '29052205361338000'
 *     },
 *     {
 *       hostname: 'D0C058',
 *       thread: Long('25'),
 *       lease: '29052205361338000'
 *     }
 *   ]
 * }
 * }</pre>
 */
@ThreadSafe
@AutoService(DistributeBarrier.class)
public final class DistributeBarrierImp extends DistributeMongoSignalBase
    implements DistributeBarrier {

  enum BarrierState {
    NONE,
    SET,
    WAIT,
    RELEASE
  }

  private final ReentrantLock lock;
  private final Condition removed;
  private final EventBus eventBus;

  public DistributeBarrierImp(
      Lease lease, String key, MongoClient mongoClient, MongoDatabase db, EventBus eventBus) {
    super(lease, key, mongoClient, db, BARRIER_NAMED);
    this.lock = new ReentrantLock();
    this.removed = lock.newCondition();
    this.eventBus = eventBus;
    this.eventBus.register(this);
  }

  @Override
  public void setBarrier() {
    checkState();
    BiFunction<ClientSession, MongoCollection<Document>, Boolean> command =
        (session, collection) ->
            collection
                    .insertOne(session, new Document("_id", this.getKey()).append("v", 1L))
                    .getInsertedId()
                != null;

    Boolean success =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(
                LockTimeout, LockBusy, LockFailed, NoSuchTransaction),
            null,
            _unused -> false);
    if (!success) throw new SignalException("Set barrier failure.");
  }

  @Override
  public void waitOnBarrier() throws InterruptedException {
    checkState();
    TxnResponse txnResponse;
    while ((txnResponse = this.doWaitOnBarrier()).txnOk
        && !txnResponse.thrownError
        && txnResponse.parkThread) {
      // 循环检查当前线程是否需要await,避免JVM虚假唤醒
      lock.lock();
      try {
        removed.await();
      } finally {
        lock.unlock();
      }
    }
    if (txnResponse.thrownError) throw new SignalException(txnResponse.message);
  }

  private TxnResponse doWaitOnBarrier() {
    Document holder = currHolder();
    BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
        (session, coll) -> {
          var filter = eq("_id", getKey());
          Document barrier;

          // 返回成功，但是不阻塞当前线程
          if ((barrier = coll.find(session, filter).first()) == null) return ok();

          long revision = barrier.getLong("v"), newRevision = revision + 1;
          var newFilter = and(filter, eq("v", revision));
          var update = combine(addToSet("o", holder), inc("v", newRevision));

          // 如果barrier为空，需要重试继续更新
          if ((barrier = coll.findOneAndUpdate(session, newFilter, update, UPDATE_OPTIONS)) == null)
            return retryableError();
          return barrier.getList("o", Document.class) != null
                  && barrier.getList("o", Document.class).contains(holder)
              ? parkThreadWithSuccess()
              : retryableError();
        };
    return commandExecutor.loopExecute(
        command,
        commandExecutor.defaultDBErrorHandlePolicy(
            LockBusy, LockFailed, LockTimeout, WriteConflict, NoSuchTransaction),
        null,
        t -> !t.txnOk && t.retryable && !t.thrownError);
  }

  @Override
  public void removeBarrier() {
    checkState();
    BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
        (session, coll) -> {
          var filter = eq("_id", getKey());

          // 如果要移除的Barrier不存在，则不向上抛出错误
          Document barrier;
          if ((barrier = coll.find(session, filter).first()) == null) return ok();
          var newFilter = and(filter, eq("v", barrier.getLong("v")));
          DeleteResult deleteResult = coll.deleteOne(session, newFilter);
          var success = deleteResult.getDeletedCount() == 1L;
          return success ? ok() : retryableError();
        };

    TxnResponse txnResponse =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(
                LockBusy, LockFailed, LockTimeout, WriteConflict, NoSuchTransaction),
            null,
            t -> !t.txnOk && t.retryable && !t.thrownError);
    if (txnResponse.thrownError) throw new SignalException(txnResponse.message);
  }

  @Override
  public int getHoldCount() {
    Document barrier =
        collection
            .aggregate(
                ImmutableList.of(
                    match(and(eq("_id", this.getKey()), type("o", BsonType.ARRAY))),
                    project(fields(computed("count", new Document("$size", "$o")), excludeId()))))
            .first();
    if (barrier == null || barrier.isEmpty()) return 0;
    return ofNullable(barrier.get("count")).map(Object::toString).map(Integer::parseInt).orElse(0);
  }

  @Override
  public Collection<Holder> getHolders() {
    Document document = collection.find(eq("_id", this.getKey())).first();
    return ofNullable(document)
        .map(doc -> doc.getList("o", Document.class))
        .map(holders -> holders.stream().map(Utils::mappedDoc2Holder).collect(toSet()))
        .orElse(emptySet());
  }

  @DoNotCall
  @Subscribe
  void awakeAll(ChangeStreamEvents.BarrierRemovedEvent event) {
    if (!this.getKey().equals(event.barrierKey())) return;
    lock.lock();
    try {
      removed.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void doClose() {
    eventBus.unregister(this);
    lock.lock();
    try {
      removed.signalAll();
    } finally {
      lock.unlock();
    }
  }
}
