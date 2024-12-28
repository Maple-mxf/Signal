package signal.mongo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.setOnInsert;
import static signal.mongo.CollectionNamed.COUNT_DOWN_LATCH_NAMED;
import static signal.mongo.MongoErrorCode.ExceededTimeLimit;
import static signal.mongo.MongoErrorCode.LockBusy;
import static signal.mongo.MongoErrorCode.LockFailed;
import static signal.mongo.MongoErrorCode.LockTimeout;
import static signal.mongo.MongoErrorCode.NoSuchTransaction;
import static signal.mongo.MongoErrorCode.TransactionExceededLifetimeLimitSeconds;
import static signal.mongo.MongoErrorCode.WriteConflict;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.Collection;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import signal.api.DistributeCountDownLatch;
import signal.api.Holder;
import signal.api.Lease;
import signal.api.SignalException;

@AutoService(DistributeCountDownLatch.class)
public final class DistributeCountDownLatchImp extends DistributeMongoSignalBase
    implements DistributeCountDownLatch {

  private static final Logger LOGGER = LoggerFactory.getLogger(DistributeCountDownLatchImp.class);

  private final int count;
  private final EventBus eventBus;

  private final ReentrantLock lock;
  private final Condition countDone;

  DistributeCountDownLatchImp(
      EventBus eventBus,
      Lease lease,
      String key,
      MongoClient mongoClient,
      MongoDatabase db,
      int count) {
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
          if (countDownLatch == null) return TxnResponse.retryableError();
          int c = countDownLatch.getInteger("c");
          if (c != this.count)
            return TxnResponse.thrownAnError(
                "Count down error. " + "Because another process is using countdownlatch resources");
          int cc = countDownLatch.getInteger("cc");
          if (cc > this.count) return TxnResponse.thrownAnError("Count down exceed.");

          return extractHolder(countDownLatch, holder).isPresent()
              ? TxnResponse.ok()
              : TxnResponse.retryableError();
        };
    TxnResponse txnResponse =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(
                ImmutableSet.of(
                    LockBusy,
                    LockFailed,
                    LockTimeout,
                    NoSuchTransaction,
                    ExceededTimeLimit,
                    TransactionExceededLifetimeLimitSeconds,
                    WriteConflict)),
            null,
            t -> !t.txnOk && t.retryable && !t.thrownError);
    if (txnResponse.txnOk) return;
    if (txnResponse.thrownError) throw new SignalException(txnResponse.message);
    throw new SignalException("Unknown Error.");
  }

  @Override
  public void await() throws InterruptedException {

    BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
        (session, coll) -> {
          Document countDownLatch = coll.find(session, eq("_id", this.getKey())).first();
          if (countDownLatch == null)
            return TxnResponse.thrownAnError("CountDownLatch instance not exists.");
          int c = countDownLatch.getInteger("c");
          if (c != this.count) {
            return TxnResponse.thrownAnError(
                String.format(
                    """
                                                              The signal registration information of CountDownLatch is inconsistent with the information of the current instance.
                                                              current count = %d,
                                                              instance of registered count = %d
                                                              """,
                    this.count, c));
          }
          return TxnResponse.ok();
        };
    TxnResponse txnResponse =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(
                ImmutableSet.of(
                    LockBusy,
                    LockFailed,
                    LockTimeout,
                    NoSuchTransaction,
                    ExceededTimeLimit,
                    TransactionExceededLifetimeLimitSeconds)),
            null,
            t -> !t.txnOk && t.retryable && !t.thrownError);
    if (txnResponse.thrownError) throw new SignalException(txnResponse.message);
    if (!txnResponse.txnOk) throw new SignalException("Unknown Error.");

    lock.lock();
    try {
      countDone.await();
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void doClose() {
    this.eventBus.unregister(this);
    lock.lock();
    try {
      countDone.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Subscribe
  void awakeAll(ChangeStreamEvents.CountDownLatchChangeEvent event) {
    if (!this.getKey().equals(event.countDownLatchKey())) return;
    if (this.count != event.c()) {
      LOGGER.error(
          """
                    Receive an CountDownLatch change event,
                    But count of CountDownLatch inconsistent. event = {} current count = {}.
                    """,
          event,
          this.count);
      return;
    }
    if (event.cc() != this.count()) return;
    lock.lock();
    try {
      countDone.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Collection<Holder> getHolders() {
    return null;
  }
}
