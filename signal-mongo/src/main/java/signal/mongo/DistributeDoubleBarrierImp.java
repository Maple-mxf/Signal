package signal.mongo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.setOnInsert;
import static signal.mongo.CollectionNamed.DOUBLE_BARRIER_NAMED;
import static signal.mongo.MongoErrorCode.DuplicateKey;
import static signal.mongo.MongoErrorCode.LockBusy;
import static signal.mongo.MongoErrorCode.LockFailed;
import static signal.mongo.MongoErrorCode.LockTimeout;
import static signal.mongo.MongoErrorCode.NoSuchTransaction;
import static signal.mongo.MongoErrorCode.WriteConflict;
import static signal.mongo.TxnResponse.ok;
import static signal.mongo.TxnResponse.retryableError;
import static signal.mongo.TxnResponse.thrownAnError;

import com.google.auto.service.AutoService;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.errorprone.annotations.DoNotCall;
import com.google.errorprone.annotations.ThreadSafe;
import com.mongodb.Function;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.result.DeleteResult;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.bson.Document;
import signal.api.DistributeDoubleBarrier;
import signal.api.Holder;
import signal.api.Lease;
import signal.api.SignalException;

/**
 * 存储格式
 *
 * <pre>{@code
 *   {
 *   _id: 'Test-Double-Barrier',
 *   o: [
 *     {
 *       hostname: 'F15557',
 *       thread: Long('25'),
 *       lease: '29072335762568000',
 *       state: 1
 *     }
 *   ],
 *   p: 4,
 *   v: Long('3')
 * }
 * }</pre>
 */
@ThreadSafe
@AutoService(DistributeDoubleBarrier.class)
final class DistributeDoubleBarrierImp extends DistributeMongoSignalBase
    implements DistributeDoubleBarrier {
  private final ReentrantLock lock;
  private final Condition entered;
  private final Condition leaved;

  private final int participants;
  private final EventBus eventBus;

  public DistributeDoubleBarrierImp(
      Lease lease,
      String key,
      MongoClient mongoClient,
      MongoDatabase db,
      int participants,
      EventBus eventBus) {
    super(lease, key, mongoClient, db, DOUBLE_BARRIER_NAMED);
    checkArgument(participants > 0, "The value of participants must be greater than 0.");

    this.lock = new ReentrantLock();
    this.entered = lock.newCondition();
    this.leaved = lock.newCondition();
    this.participants = participants;
    this.eventBus = eventBus;
    this.eventBus.register(this);
  }

  @Override
  public int participants() {
    return participants;
  }

  @Override
  public void enter() throws InterruptedException {
    checkState();
    TxnResponse txnResponse = this.doEnter();
    if (txnResponse.thrownError) {
      if (lock.isLocked() && lock.isHeldByCurrentThread()) lock.unlock();
      throw new SignalException(txnResponse.message);
    }
    try {
      entered.await();
    } finally {
      lock.unlock();
    }
  }

  private TxnResponse doEnter() {
    Document holder = getCurrHolder(1);
    BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
        (session, coll) -> {
          Document doubleBarrier;
          if ((doubleBarrier =
                  coll.findOneAndUpdate(
                      session,
                      eq("_id", this.getKey()),
                      combine(
                          setOnInsert("p", this.participants()),
                          addToSet("o", holder),
                          inc("v", 1L)),
                      UPSERT_OPTIONS))
              == null) return retryableError();

          int p = doubleBarrier.getInteger("p");
          if (p != this.participants)
            return thrownAnError(
                String.format(
                    """
                    The current number of participants is %d, which is not equal to the set number of %d.
                    """,
                    this.participants, p));

          List<Document> holders = doubleBarrier.getList("o", Document.class);
          if (holders == null) return retryableError();
          if (holders.size() > this.participants)
            return thrownAnError("The current number of participants is overflow.");

          Optional<Document> optional = extractHolder(doubleBarrier, holder);
          if (optional.isEmpty()) return retryableError();

          if (optional.get().getInteger("state") != 1)
            return thrownAnError("The current phase is leaved.");
          // 提交事务之前，为避免awakeAll函数先执行，在TXN事务内部加锁，扩大锁覆盖的临界区
          // lock.lock();
          return ok();
        };
    return commandExecutor.loopExecute(
        command,
        commandExecutor.defaultDBErrorHandlePolicy(
            LockTimeout, LockBusy, LockFailed, NoSuchTransaction, WriteConflict, DuplicateKey),
        null,
        t -> !t.txnOk && t.retryable && !t.thrownError);
  }

  @Override
  public void leave() throws InterruptedException {
    checkState();
    Document holder = getCurrHolder(0);
    BiFunction<ClientSession, MongoCollection<Document>, TxnResponse> command =
        (session, coll) -> {
          var filter =
              and(
                  eq("_id", this.getKey()),
                  eq("p", this.participants),
                  elemMatch(
                      "o",
                      and(
                          eq("hostname", holder.get("hostname")),
                          eq("thread", holder.get("thread")),
                          eq("lease", holder.get("lease")))));
          var update = combine(set("o.$.state", 0), inc("v", 1L));
          Document doubleBarrier = coll.findOneAndUpdate(session, filter, update, UPDATE_OPTIONS);
          Optional<Document> optional;
          if (doubleBarrier == null || (optional = extractHolder(doubleBarrier, holder)).isEmpty())
            return thrownAnError(
                "The current thread has not entered the enter state or there are other DoubleBarrier instances running?");

          if (optional.get().getInteger("state") != 0) return thrownAnError("Unknown Error.");

          // 到达删除数据的条件
          if (doubleBarrier.getList("o", Document.class).stream()
              .allMatch(t -> t.getInteger("state") == 0)) {
            DeleteResult deleteResult =
                coll.deleteOne(
                    session, and(eq("_id", getKey()), eq("v", doubleBarrier.getLong("v"))));
            if (deleteResult.getDeletedCount() == 1L) {
              lock.lock();
              return ok();
            }
            return retryableError();
          }
          // 提交事务之前，为避免awakeAll函数先执行，在TXN事务内部加锁，扩大锁覆盖的临界区
          // 由于锁的可重入性质，Lambda外部代码可以重复Lock
          lock.lock();
          return ok();
        };
    TxnResponse txnResponse =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(
                LockBusy, LockFailed, LockTimeout, NoSuchTransaction, WriteConflict),
            null,
            t -> !t.txnOk && t.retryable && !t.thrownError);

    if (txnResponse.thrownError) {
      if (lock.isLocked() && lock.isHeldByCurrentThread()) lock.unlock();
      throw new SignalException(txnResponse.message);
    }
    try {
      leaved.await();
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void doClose() {
    this.eventBus.unregister(this);
  }

  private Document getCurrHolder(int state) {
    Document holder = currHolder();
    holder.put("state", state);
    return holder;
  }

  /**
   * 在Update场景下，{@link ChangeStreamDocument#getFullDocument()}空的场景如下 在一个事务内部， 包含A和B两个操作(A和B顺序执行)
   * A：修改 id = '123' 的数据 B：删除 id = '123' 的数据 以上两个操作会导致MongoDB发布两次ChangeStream Event事件
   * 第一次事件是UPDATE，在UPDATE事件提供的{@link ChangeStreamDocument#getFullDocument()}会为空，第二次是DELETE
   *
   * @param event double barrier的更新操作，不监听删除事件
   */
  @DoNotCall
  @Subscribe
  void awakeAll(ChangeStreamEvents.DoubleBarrierChangeEvent event) {
    if (!this.getKey().equals(event.doubleBarrierKey())
        || event.participants() != this.participants()) return;

    Consumer<Condition> awakeFn =
        cond -> {
          lock.lock();
          try {
            cond.signalAll();
          } finally {
            lock.unlock();
          }
        };

    Document fullDocument = event.fullDocument();
    // 代表删除操作
    if (fullDocument == null) {
      awakeFn.accept(leaved);
      return;
    }

    List<Document> holders = fullDocument.getList("o", Document.class);
    if (holders == null || holders.size() != this.participants()) return;

    Function<Boolean, Integer> computePhaseFn =
        (flag) ->
            holders.stream()
                .map(t -> t.getInteger("state"))
                .reduce((state1, state2) -> flag ? state1 & state2 : state1 | state2)
                .get();

    if (computePhaseFn.apply(true) == 1) {
      lock.lock();
      try {
        entered.signalAll();
      } finally {
        lock.unlock();
      }
    } else if (computePhaseFn.apply(false) == 0) {
      lock.lock();
      try {
        leaved.signalAll();
      } finally {
        lock.unlock();
      }
    }
  }

  @Override
  public Collection<Holder> getHolders() {
    return super.doGetHolders();
  }
}
