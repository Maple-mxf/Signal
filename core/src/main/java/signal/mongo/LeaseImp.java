package signal.mongo;

import static com.mongodb.client.model.Filters.eq;

import com.google.auto.service.AutoService;
import com.google.common.eventbus.EventBus;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.bson.Document;
import signal.api.DistributeBarrier;
import signal.api.DistributeCountDownLatch;
import signal.api.DistributeDoubleBarrier;
import signal.api.DistributeMutexLock;
import signal.api.DistributeReadWriteLock;
import signal.api.DistributeSemaphore;
import signal.api.Lease;

@AutoService(Lease.class)
final class LeaseImp extends Lease {
  private final MongoClient client;
  private final MongoDatabase db;
  private final MongoCollection<Document> collection;
  private final Map<String, DistributeMongoSignalBase<?>> signalList = new HashMap<>(4);
  private final EventBus leaseScopedEventBus;

  private boolean revoked = false;

  LeaseImp(MongoClient client, MongoDatabase db, String leaseID) {
    super(leaseID, Instant.now(Clock.systemUTC()));
    this.client = client;
    this.db = db;
    this.leaseScopedEventBus = new EventBus(leaseID);
    this.collection = db.getCollection(CollectionNamed.LEASE_NAMED);

    Instant now = this.getCreatedTime();
    InsertOneResult insertOneResult =
        this.collection.insertOne(
            new Document("_id", leaseID)
                .append("createAt", now)
                .append("expireAt", now.plusSeconds(30)));

    // TODO UnusedVariable
    System.out.println(insertOneResult);
  }

  void postScopedEvent(Object event) {
    this.leaseScopedEventBus.post(event);
  }

  @Override
  public synchronized void revoke() {
    if (revoked) return;
    for (DistributeMongoSignalBase<?> signal : signalList.values()) signal.close();
    this.doRevoke();
    revoked = true;
  }

  @Override
  public boolean isRevoked() {
    return revoked;
  }

  private synchronized void doRevoke() {
    DeleteResult deleteResult = collection.deleteOne(eq("_id", this.id));
    System.out.println(deleteResult);
  }

  @Override
  public synchronized DistributeCountDownLatch getCountDownLatch(String key, int count) {
    checkTypesafe(key, DistributeCountDownLatchImp.class);
    return (DistributeCountDownLatch)
        signalList.computeIfAbsent(
            key,
            _unusedK ->
                new DistributeCountDownLatchImp(
                    this, key, this.client, this.db, count, leaseScopedEventBus));
  }

  @Override
  public synchronized DistributeMutexLock getMutexLock(String key) {
    checkTypesafe(key, DistributeMutexLockImp.class);
    return (DistributeMutexLock)
        signalList.computeIfAbsent(
            key,
            _unusedK -> new DistributeMutexLockImp(this, key, client, db, leaseScopedEventBus));
  }

  @Override
  public synchronized DistributeReadWriteLock getReadWriteLock(String key) {
    checkTypesafe(key, DistributeReadWriteLockImp.class);
    return (DistributeReadWriteLock)
        signalList.computeIfAbsent(
            key,
            _unusedK -> new DistributeReadWriteLockImp(this, key, client, db, leaseScopedEventBus));
  }

  @Override
  public synchronized DistributeSemaphore getSemaphore(String key, int permits) {
    checkTypesafe(key, DistributeSemaphoreImp.class);
    return (DistributeSemaphoreImp)
        signalList.computeIfAbsent(
            key,
            _unusedK ->
                new DistributeSemaphoreImp(this, key, client, db, permits, leaseScopedEventBus));
  }

  @Override
  @SuppressWarnings("CompileTimeConstant")
  public synchronized DistributeBarrier getBarrier(String key) {
    checkTypesafe(key, DistributeBarrierImp.class);
    return (DistributeBarrier)
        signalList.computeIfAbsent(
            key, _unusedK -> new DistributeBarrierImp(this, key, client, db, leaseScopedEventBus));
  }

  @Override
  public synchronized DistributeDoubleBarrier getDoubleBarrier(String key, int participants) {
    checkTypesafe(key, DistributeDoubleBarrierImp.class);
    return (DistributeDoubleBarrier)
        this.signalList.computeIfAbsent(
            key,
            _unusedK ->
                new DistributeDoubleBarrierImp(
                    this, key, client, db, participants, leaseScopedEventBus));
  }

  private void checkTypesafe(String key, Class<? extends DistributeMongoSignalBase<?>> type) {
    DistributeMongoSignalBase<?> signal = signalList.get(key);
    if (signal == null) return;
    if (!signal.getClass().equals(type))
      throw new UnsupportedOperationException(
          String.format(
              "signal key invalid. Because the Signal represented by key already has an instance object = [%s]",
              signal.getClass().getSimpleName()));
  }
}
