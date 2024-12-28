package signal.mongo;

import com.google.auto.service.AutoService;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import signal.api.Holder;
import signal.api.Lease;
import signal.api.DistributeMutexLock;

import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

@AutoService(DistributeMutexLock.class)
public final class DistributeMutexLockImp extends DistributeMongoSignalBase
    implements DistributeMutexLock {

  public DistributeMutexLockImp(
      Lease lease, String key, MongoClient mongoClient, MongoDatabase db) {
    super(lease, key, mongoClient, db, CollectionNamed.MUTEX_LOCK_NAMED);
  }

  @Override
  public boolean tryLock(Long waitTime, TimeUnit timeUnit) {
    return false;
  }

  @Override
  public void unlock() {}

  @Override
  public boolean isLocked() {
    return collection.countDocuments(eq("_id", this.getKey())) == 1L;
  }

  @Override
  public boolean isHeldByCurrentThread() {
    Document holder = currHolder();
    return collection.countDocuments(and(eq("_id", this.getKey()), eq("o", holder))) == 1L;
  }

  @Override
  public Holder getHolder() {
    return super.doGetFirstHolder();
  }

  @Override
  protected void doClose() {}
}
