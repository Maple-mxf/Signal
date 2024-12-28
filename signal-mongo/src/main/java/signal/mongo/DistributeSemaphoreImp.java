package signal.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.pull;
import static com.mongodb.client.model.Updates.set;
import static java.lang.System.nanoTime;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toSet;
import static signal.mongo.MongoErrorCode.DuplicateKey;
import static signal.mongo.MongoErrorCode.ExceededTimeLimit;
import static signal.mongo.MongoErrorCode.LockFailed;
import static signal.mongo.MongoErrorCode.NoSuchTransaction;
import static signal.mongo.MongoErrorCode.TransactionExceededLifetimeLimitSeconds;
import static signal.mongo.MongoErrorCode.WriteConflict;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import signal.api.DistributeSemaphore;
import signal.api.Holder;
import signal.api.Lease;
import signal.api.SignalException;

/**
 * 信号量维护一个整数计数值，这个值表示可用资源的数量或许可证数。
 *
 * <p>信号量支持两个基本操作
 *
 * <ul>
 *   <li>
 *       <p>P（Proberen） 或 acquire()（减操作）
 *       <ul>
 *         <li>线程请求一个许可证（即，尝试获取资源）。
 *         <li>如果信号量的计数大于 0，线程可以成功获取一个许可证，信号量的计数减 1。
 *         <li>如果信号量的计数为 0，线程将被阻塞，直到有其他线程释放一个许可证。
 *       </ul>
 *   <li>
 *       <p>V（Verhogen） 或 release()（加操作）：
 *       <ul>
 *         <li>线程释放一个许可证（即，释放资源）
 *         <li>信号量的计数加 1，表示一个资源变得可用
 *         <li>如果有其他线程在等待许可证，则被唤醒并允许继续执行
 *       </ul>
 * </ul>
 *
 * <p>信号量内部会维护一个队列存储等待的线程信息存储阻塞等待的线程信息
 *
 * <p>Semaphore存储格式
 *
 * <pre>
 * {@code [
 *   {
 *     _id: 'Test-Semaphore1',
 *     p: 4,
 *     o: [
 *       {
 *         hostname: '79b33a',
 *         thread: Long('33'),
 *         lease: '26122259844800000',
 *         acquire_permits: 1
 *       },
 *     ],
 *     v: Long('1')
 *   }
 * ]}
 *     </pre>
 *
 * 信号的公平性：公平
 */
@AutoService({DistributeSemaphore.class})
public final class DistributeSemaphoreImp extends DistributeMongoSignalBase
    implements DistributeSemaphore {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributeSemaphoreImp.class);
  private final EventBus eventBus;

  private final int permits;

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition notFull = lock.newCondition();

  // 建立内存屏障
  // 确保写操作的顺序性：防止某些写操作被重新排序到屏障之前
  // 确保读操作的顺序性：防止某些读操作被重新排序到屏障之后。
  // 保证线程间的内存可见性：确保在某个线程中进行的写操作对其他线程是可见的。
  private volatile int occupiedPermitsCount;

  DistributeSemaphoreImp(
      Lease lease,
      String key,
      MongoClient mongoClient,
      MongoDatabase db,
      int permits,
      EventBus eventBus) {
    super(lease, key, mongoClient, db, CollectionNamed.SEMAPHORE_NAMED);
    this.eventBus = eventBus;
    this.eventBus.register(this);
    this.permits = permits;
    this.occupiedPermitsCount = 0;
  }

  private record ModOccupiedPermitsArgs(int oldVal, int newVal) {}

  private record AcquireTxnResult(
      boolean txnOk,
      boolean parkThread,
      boolean throwError,
      String message,
      ModOccupiedPermitsArgs modOccupiedPermitsArgs) {}

  private record ReleaseTxnResult(boolean txnOk, boolean retryable, String message) {}

  @Override
  public void acquire(int permits, Long waitTime, TimeUnit timeUnit) throws InterruptedException {
    checkState();
    Preconditions.checkArgument(
        permits <= this.permits(),
        String.format(
            "The requested permits [%d] exceed the limit [%d].", permits, this.permits()));

    Document holder = currHolder();
    holder.put("acquire_permits", permits);

    BiFunction<ClientSession, MongoCollection<Document>, AcquireTxnResult> command =
        (session, collection) -> {
          Document sem = collection.find(session, eq("_id", this.getKey())).first();
          if (sem == null) {
            InsertOneResult insertResult =
                collection.insertOne(
                    session,
                    new Document("_id", this.getKey())
                        .append("p", this.permits())
                        .append("o", ImmutableList.of(holder))
                        .append("v", 1L));

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Create semaphore success {}", insertResult);
            }
            return new AcquireTxnResult(true, false, false, "", null);
          }
          int p = sem.getInteger("p");
          if (p != this.permits()) {
            return new AcquireTxnResult(
                false, false, true, "Semaphore permits inconsistency.", null);
          }

          // 已占用的permits数量，如果可用数量小于申请的数量，则Blocking当前Thread
          int occupied =
              sem.getList("o", Document.class).stream()
                  .mapToInt(t -> t.getInteger("acquire_permits"))
                  .sum();
          int available = this.permits() - occupied;
          if (available < permits) {
            return new AcquireTxnResult(
                false, true, false, "", new ModOccupiedPermitsArgs(occupiedPermitsCount, occupied));
          }

          Optional<Document> optional = extractHolder(sem, holder);
          if (optional.isPresent()) {
            int acquired = optional.get().getInteger("acquire_permits");
            if ((acquired + permits) > this.permits()) {
              return new AcquireTxnResult(
                  false, false, true, "Maximum permit count exceeded", null);
            }
          }

          var newRevision = sem.getLong("v") + 1;
          var filter =
              optional.isPresent()
                  ? and(
                      eq("_id", this.getKey()),
                      eq("v", sem.get("v")),
                      elemMatch("o", Utils.mappedHolder2AndFilter(holder)))
                  : and(eq("_id", this.getKey()), eq("v", sem.get("v")));
          var updates =
              optional
                  .map(
                      value ->
                          combine(
                              set(
                                  "o.$.acquire_permits",
                                  value.getInteger("acquire_permits") + permits),
                              inc("v", 1)))
                  .orElseGet(() -> combine(addToSet("o", holder), inc("v", 1L)));

          sem = collection.findOneAndUpdate(session, filter, updates, UPDATE_OPTIONS);
          if (sem == null) return new AcquireTxnResult(false, false, false, "", null);
          return (extractHolder(sem, holder).isPresent() && sem.getLong("v") == newRevision)
              ? new AcquireTxnResult(true, false, false, "", null)
              : new AcquireTxnResult(false, true, false, "", null);
        };

    long s = nanoTime(), waitTimeNanos = timeUnit.toNanos(waitTime);
    boolean timed = waitTime > 0;

    for (; ; ) {

      parkCurrentThread(timed, waitTimeNanos, s);

      checkState();

      AcquireTxnResult txnResult =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                  Set.of(
                      NoSuchTransaction,
                      ExceededTimeLimit,
                      DuplicateKey,
                      LockFailed,
                      TransactionExceededLifetimeLimitSeconds)),
              null,
              t -> false,
              timed,
              timed ? (waitTimeNanos - (nanoTime() - s)) : -1L,
              NANOSECONDS);

      if (txnResult.txnOk) {
        unparkSuccessor();
        return;
      }

      // Unexpected error
      if (txnResult.throwError) {
        throw new SignalException(txnResult.message);
      }

      // Timeout
      if (timed && (nanoTime() - s) >= waitTimeNanos) {
        throw new SignalException("Acquire timeout.");
      }

      // 唤醒操作和阻塞操作执行顺序分析
      // 第一种情况
      // A awakeHead函数先执行，修改了occupiedPermitsCount的值
      //   A-A 若修改后的occupiedPermitsCount的值小于permits,
      //       此时parkCurrentThread并不会先await，因为occupiedPermitsCount的值
      //       不满足阻塞线程继续申请permits，因为occupiedPermitsCount的值表明获取
      //       permits的条件成立，所以结果符合预期
      //   A-B 若修改后的occupiedPermitsCount的值大于等于permits,
      //       则当前线程阻塞条件成立，进入下一轮循环，线程挂起

      // 第二种情况
      // B 当前线程先执行阻塞操作，awakeHead函数后执行，此时awakeHead函数会唤醒队列中的第一个阻塞的线程
      //   B-A 若第一个阻塞的线程申请permits成功，则唤醒队列中的下一个阻塞的线程，
      //       下一个阻塞的线程继续获取permits
      //   B-B 若第一个阻塞的线程申请permits失败并且满足阻塞条件，则进入阻塞状态，awakeHead函数将唤醒
      //       第一个阻塞的线程
      if (txnResult.parkThread) {
        // tryModPermits2FullState(txnResult);
        Thread.onSpinWait();
      }
    }
  }

  /**
   * 尝试修改{@link DistributeSemaphoreImp#occupiedPermitsCount}的值，在分布式环境中，
   * awakeHead操作可能相对迟缓，因为存在网络数据传输的延迟，尝试修改occupiedPermitsCount 值有助于提升其他线程无效的Acquire事务尝试操作
   *
   * @param txnResult Acquire事务运行结果
   */
  private void tryModPermits2FullState(AcquireTxnResult txnResult) {
    lock.lock();
    if (occupiedPermitsCount == txnResult.modOccupiedPermitsArgs.oldVal
        && occupiedPermitsCount != txnResult.modOccupiedPermitsArgs.newVal) {
      occupiedPermitsCount = txnResult.modOccupiedPermitsArgs.newVal;
    }
    lock.unlock();
  }

  private void parkCurrentThread(boolean timed, long waitTimeNanos, long s)
      throws InterruptedException {
    lock.lock();
    try {
      while (occupiedPermitsCount == this.permits()) {
        // await函数触发释放锁，这里不会造成当前线程长时间持有锁造成awakeHead函数阻塞
        // 唯一存在Lock竞争的位置是awakeHead
        if (timed) {
          boolean elapsed = notFull.await((waitTimeNanos - (nanoTime() - s)), NANOSECONDS);
          if (!elapsed) throw new SignalException("Acquire Timeout.");
        } else {
          notFull.await();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void release(int permits) {
    checkState();
    Preconditions.checkArgument(
        permits <= this.permits(),
        String.format(
            "The requested permits [%d] exceed the limit [%d].", permits, this.permits()));

    var holder = currHolder();
    BiFunction<ClientSession, MongoCollection<Document>, ReleaseTxnResult> command =
        (session, collection) -> {
          Document sem = collection.find(session, eq("_id", this.getKey())).first();
          if (sem == null || sem.getInteger("p") != this.permits())
            return new ReleaseTxnResult(
                false, false, String.format("Semaphore not exists. key = %s", this.getKey()));

          int p = sem.getInteger("p");
          if (p != this.permits())
            return new ReleaseTxnResult(false, false, "Semaphore permits inconsistency.");

          Optional<Document> optional = extractHolder(sem, holder);
          if (optional.isEmpty())
            return new ReleaseTxnResult(
                false, false, "The current thread does not hold a permits.");

          var actualHolder = optional.get();
          int acquired = actualHolder.getInteger("acquire_permits");
          int unreleased = acquired - permits;
          if (unreleased < 0)
            return new ReleaseTxnResult(
                false,
                false,
                String.format(
                    "The current thread holds %d permits and is not allowed to release %d permits",
                    acquired, permits));

          var filter = and(eq("_id", this.getKey()), eq("v", sem.get("v")));

          if (unreleased == 0L && sem.getList("o", Document.class).size() == 1) {
            DeleteResult deleteResult = collection.deleteOne(session, filter);
            var success = deleteResult.getDeletedCount() == 1L;
            return new ReleaseTxnResult(success, !success, "");
          }

          var newRevision = sem.getLong("v") + 1;
          var update =
              unreleased > 0
                  ? combine(inc("v", 1), set("o.$.acquire_permits", unreleased))
                  : combine(inc("v", 1), pull("o", Utils.mappedHolder2AndFilter(holder)));
          sem = collection.findOneAndUpdate(session, filter, update, UPDATE_OPTIONS);
          var success = sem != null && sem.getLong("v") == newRevision;
          return new ReleaseTxnResult(success, !success, "update");
        };

    Predicate<ReleaseTxnResult> resRetryablePolicy =
        txnResult -> !txnResult.txnOk && txnResult.retryable;

    ReleaseTxnResult txnResult =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(
                ImmutableSet.of(NoSuchTransaction, WriteConflict)),
            null,
            resRetryablePolicy);

    LOGGER.debug("Release result {} {}", txnResult, Thread.currentThread().getId());

    if (!txnResult.txnOk) throw new SignalException(txnResult.message);
  }

  public void forceReleaseAll() {
    BiFunction<ClientSession, MongoCollection<Document>, ReleaseTxnResult> command =
        (session, collection) -> {
          Document sem = collection.find(session, eq("_id", this.getKey())).first();
          if (sem == null) return new ReleaseTxnResult(true, false, "");
          if (this.permits() != sem.getInteger("p"))
            return new ReleaseTxnResult(false, false, "Semaphore permits inconsistency.");

          List<Document> holders = ofNullable(sem.getList("o", Document.class)).orElse(emptyList());
          if (holders.stream()
              .noneMatch(t -> this.getLease().getLeaseID().equals(t.getString("lease")))) {
            return new ReleaseTxnResult(true, false, "");
          }
          var filter =
              and(eq("_id", this.getKey()), eq("v", sem.getLong("v")), eq("p", this.permits()));
          if (holders.isEmpty()
              || holders.stream()
                  .allMatch(t -> t.getString("lease").equals(getLease().getLeaseID()))) {
            DeleteResult deleteResult = collection.deleteOne(session, filter);
            var success = deleteResult.getDeletedCount() == 1L;
            return new ReleaseTxnResult(success, !success, "");
          }

          var update = combine(pull("o", eq("lease", getLease().getLeaseID())), inc("v", 1));
          var newRevision = sem.getLong("v") + 1L;
          sem = collection.findOneAndUpdate(session, filter, update);
          var success = (sem != null && sem.getLong("v") == newRevision);
          return new ReleaseTxnResult(success, !success, "");
        };

    ReleaseTxnResult txnResult =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(
                ImmutableSet.of(
                    NoSuchTransaction,
                    ExceededTimeLimit,
                    WriteConflict,
                    LockFailed,
                    TransactionExceededLifetimeLimitSeconds)),
            null,
            t -> !t.txnOk && t.retryable);
    if (!txnResult.txnOk) throw new SignalException(txnResult.message);
  }

  private void unparkSuccessor() {
    lock.lock();
    try {
      // 只唤醒头部节点
      if (occupiedPermitsCount < permits()) {
        notFull.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * 接受ChangeStream删除或者更新事件，接收到后唤醒头部节点
   *
   * @param event 接受{@link com.mongodb.client.model.changestream.OperationType#DELETE} 或者{@link
   *     com.mongodb.client.model.changestream.OperationType#UPDATE}事件
   */
  @Subscribe
  void awakeHead(ChangeStreamEvents.SemaphoreChangeAndRemovedEvent event) {
    if (!this.getKey().equals(event.semaphoreKey()) || this.permits() != event.permits()) return;

    lock.lock();
    try {
      int occupiedPermits =
          ofNullable(event.fullDocument())
              .map(t -> t.getList("o", Document.class))
              .map(h -> h.stream().mapToInt(t -> t.getInteger("acquire_permits")).sum())
              .orElse(0);
      occupiedPermitsCount = occupiedPermits;

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Receive an SemaphoreChangeEvent {} occupiedPermitsCount {}",
            event,
            occupiedPermitsCount);
      }
      if (occupiedPermits >= this.permits()) return;
      notFull.signal();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int permits() {
    return permits;
  }

  @Override
  public Collection<Holder> getHolders() {
    Document record = collection.find(eq("_id", this.getKey())).first();
    List<Document> owners;
    if (record == null || (owners = record.getList("o", Document.class)) == null) return emptySet();
    return owners.stream().map(Utils::mappedDoc2Holder).collect(toSet());
  }

  @Override
  protected void doClose() {
    this.eventBus.unregister(this);
    this.forceReleaseAll();
    lock.lock();
    try {
      notFull.signalAll();
    } finally {
      lock.unlock();
    }
  }
}
