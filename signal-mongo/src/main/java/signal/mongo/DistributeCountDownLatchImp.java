package signal.mongo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.setOnInsert;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static signal.mongo.CollectionNamed.COUNT_DOWN_LATCH_NAMED;
import static signal.mongo.MongoErrorCode.LockFailed;
import static signal.mongo.MongoErrorCode.NoSuchTransaction;
import static signal.mongo.MongoErrorCode.WriteConflict;
import static signal.mongo.TxnResponse.ok;
import static signal.mongo.TxnResponse.retryableError;
import static signal.mongo.TxnResponse.thrownAnError;
import static signal.mongo.Utils.unparkSuccessor;

import com.google.auto.service.AutoService;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.errorprone.annotations.DoNotCall;
import com.google.errorprone.annotations.ThreadSafe;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import org.bson.Document;
import signal.api.DistributeCountDownLatch;
import signal.api.Holder;
import signal.api.Lease;
import signal.api.SignalException;

@ThreadSafe
@AutoService(DistributeCountDownLatch.class)
final class DistributeCountDownLatchImp extends DistributeMongoSignalBase
    implements DistributeCountDownLatch {
  private final int count;
  private final EventBus eventBus;

  private final ReentrantLock lock;
  private final Condition countDone;

  DistributeCountDownLatchImp(
      Lease lease,
      String key,
      MongoClient mongoClient,
      MongoDatabase db,
      int count,
      EventBus eventBus) {
    super(lease, key, mongoClient, db, COUNT_DOWN_LATCH_NAMED);
    this.count = count;
    checkArgument(count > 0, "The value of count must be greater than 0.");

    this.lock = new ReentrantLock();
    this.countDone = lock.newCondition();

    this.eventBus = eventBus;
    this.eventBus.register(this);
  }

  @Override
  public int count() {
    return count;
  }

  @Override
  public void countDown() {
    Document holder = currHolder();

    BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
        (session, coll) -> {
          Document countDownLatch =
              coll.findOneAndUpdate(
                  session,
                  eq("_id", this.getKey()),
                  combine(addToSet("o", holder), setOnInsert("c", this.count), inc("cc", 1)),
                  UPSERT_OPTIONS);
          if (countDownLatch == null) return retryableError();
          if (countDownLatch.getInteger("c") != this.count)
            return thrownAnError(
                "Count down error. Because another process is using count down latch resources");
          int cc = countDownLatch.getInteger("cc");
          if (cc > this.count) return thrownAnError("Count down exceed.");
          return extractHolder(countDownLatch, holder).isPresent() ? ok() : retryableError();
        };
    TxnResponse rsp =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(
                LockFailed, NoSuchTransaction, WriteConflict),
            null,
            t -> !t.txnOk && t.retryable && !t.thrownError);
    if (rsp.txnOk) return;
    if (rsp.thrownError) throw new SignalException(rsp.message);
    throw new SignalException("Unknown Error.");
  }

  @Override
  public void await() throws InterruptedException {
    this.await(-1L, NANOSECONDS);
  }

  @Override
  public void await(long waitTime, TimeUnit timeUnit) throws InterruptedException {
    BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
        (session, coll) -> {
          Document countDownLatch = coll.find(session, eq("_id", this.getKey())).first();
          if (countDownLatch == null) return thrownAnError("CountDownLatch instance not exists.");
          int c = countDownLatch.getInteger("c");
          if (c != this.count) {
            return thrownAnError(
                String.format(
                    """
                                                                          The signal registration information of CountDownLatch is inconsistent with the information of the current instance.
                                                                          current count = %d,
                                                                          instance of registered count = %d
                                                                          """,
                    this.count, c));
          }

          return ok();
        };
    TxnResponse rsp =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(LockFailed, NoSuchTransaction),
            null,
            t -> !t.txnOk && t.retryable && !t.thrownError);
    if (rsp.thrownError) {
      if (lock.isLocked() && lock.isHeldByCurrentThread()) lock.unlock();
      throw new SignalException(rsp.message);
    }

    lock.lock();
    try {
      boolean timed = waitTime > 0L;
      if (timed) {
        if (!countDone.await(waitTime, timeUnit)) throw new SignalException("Timeout.");
      } else {
        countDone.await();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void doClose() {
    this.eventBus.unregister(this);
    unparkSuccessor(lock, countDone, true);
  }

  @DoNotCall
  @Subscribe
  void awakeAll(ChangeStreamEvents.CountDownLatchChangeEvent event) {
    if (!this.getKey().equals(event.countDownLatchKey())) return;
    if (this.count != event.c()) return;
    if (event.cc() != this.count()) return;
    unparkSuccessor(lock, countDone, true);
  }

  @Override
  public Collection<Holder> getHolders() {
    return super.doGetHolders();
  }
}
