package signal.mongo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.pull;
import static com.mongodb.client.model.Updates.set;
import static java.lang.System.identityHashCode;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static signal.mongo.CollectionNamed.SEMAPHORE_NAMED;
import static signal.mongo.CommonTxnResponse.ok;
import static signal.mongo.CommonTxnResponse.retryableError;
import static signal.mongo.CommonTxnResponse.thrownAnError;
import static signal.mongo.MongoErrorCode.DuplicateKey;
import static signal.mongo.MongoErrorCode.LockBusy;
import static signal.mongo.MongoErrorCode.LockFailed;
import static signal.mongo.MongoErrorCode.LockTimeout;
import static signal.mongo.MongoErrorCode.NoSuchTransaction;
import static signal.mongo.MongoErrorCode.WriteConflict;
import static signal.mongo.Utils.parkCurrentThreadUntil;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.bson.Document;
import org.bson.conversions.Bson;
import signal.api.DistributeSemaphore;
import signal.api.Lease;
import signal.api.SignalException;
import signal.mongo.pojo.SemaphoreDocument;
import signal.mongo.pojo.SemaphoreOwnerDocument;

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
 *         lease: '26122259844800000',
 *         acquire_permits: 1
 *       },
 *     ],
 *     v: Long('1')
 *   }
 * ]}
 *     </pre>
 *
 * <p>信号的公平性：公平
 */
@ThreadSafe
@AutoService({DistributeSemaphore.class})
public class DistributeSemaphoreImp extends DistributeMongoSignalBase<SemaphoreDocument>
    implements DistributeSemaphore {
  private final EventBus eventBus;
  private final int permits;

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition notFull = lock.newCondition();

  private final boolean allowReleaseWithoutAcquire;

  // 建立内存屏障
  // 确保写操作的顺序性：防止某些写操作被重新排序到屏障之前
  // 确保读操作的顺序性：防止某些读操作被重新排序到屏障之后。
  // 保证线程间的内存可见性：确保在某个线程中进行的写操作对其他线程是可见的。
  @Keep
  @GuardedBy("varHandle")
  @VisibleForTesting
  StatefulVar<Integer> statefulVar;

  // Acquire 语义
  // 1. 保证当前线程在交换操作后，能够“获得”并看到所有其他线程在交换之前已经做出的更新。
  // 2. 确保当前线程执行该交换操作之后，后续的读取操作能够看到正确的共享数据状态。
  // Release 语义
  // 1. 保证当前线程在交换操作之前，对共享变量的所有写操作（即交换操作之前的写操作）都已经完成，并且这些写操作对其他线程是可见的。
  // 2. 确保当前线程执行该交换操作之前，所有对共享数据的写操作已经对其他线程“发布”，使得其他线程能够正确看到这些更新。
  private final VarHandle varHandle;

  DistributeSemaphoreImp(
      Lease lease,
      String key,
      MongoClient mongoClient,
      MongoDatabase db,
      int permits,
      EventBus eventBus) {
    this(lease, key, mongoClient, db, permits, eventBus, true);
  }

  DistributeSemaphoreImp(
      Lease lease,
      String key,
      MongoClient mongoClient,
      MongoDatabase db,
      int permits,
      EventBus eventBus,
      boolean allowReleaseWithoutAcquire) {
    super(lease, key, mongoClient, db, SEMAPHORE_NAMED);
    this.allowReleaseWithoutAcquire = allowReleaseWithoutAcquire;
    this.eventBus = eventBus;
    this.eventBus.register(this);
    this.permits = permits;
    this.statefulVar = new StatefulVar<>(0);
    try {
      this.varHandle =
          MethodHandles.lookup()
              .findVarHandle(DistributeSemaphoreImp.class, "statefulVar", StatefulVar.class);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException();
    }
  }

  private SemaphoreOwnerDocument buildCurrentOwner(int permits) {
    return new SemaphoreOwnerDocument(
        Utils.getCurrentHostname(),
        this.getLease().getLeaseID(),
        allowReleaseWithoutAcquire ? "" : Utils.getCurrentThreadName(),
        permits);
  }

  private Bson buildOwnerFilter(SemaphoreOwnerDocument owner) {
    List<Bson> andConditions = new ArrayList<>(8);
    andConditions.add(eq("hostname", owner.hostname()));
    andConditions.add(eq("lease", owner.lease()));
    if (!allowReleaseWithoutAcquire) andConditions.add(eq("thread", owner.thread()));
    return and(andConditions);
  }

  @Override
  public void acquire(int permits, Long waitTime, TimeUnit timeUnit) throws InterruptedException {
    checkState();
    checkArgument(
        permits <= this.permits(),
        String.format(
            "The requested permits [%d] exceed the limit [%d].", permits, this.permits()));

    SemaphoreOwnerDocument thisOwner = this.buildCurrentOwner(permits);

    BiFunction<ClientSession, MongoCollection<SemaphoreDocument>, CommonTxnResponse> command =
        (session, collection) -> {
          @SuppressWarnings("unchecked")
          StatefulVar<Integer> currState = (StatefulVar<Integer>) varHandle.getAcquire(this);
          int currStateHashCode = identityHashCode(currState);

          SemaphoreDocument sd = collection.find(session, eq("_id", this.getKey())).first();
          if (sd == null) {
            sd = new SemaphoreDocument(this.key, this.permits, ImmutableList.of(thisOwner), 1L);
            InsertOneResult insertResult = collection.insertOne(session, sd);
            return insertResult.getInsertedId() != null ? ok() : retryableError();
          }
          if (sd.permits() != this.permits())
            return thrownAnError("Semaphore permits inconsistency.");

          // 已占用的permits数量，如果可用数量小于申请的数量，则Blocking当前Thread
          int occupied = sd.owners().stream().mapToInt(SemaphoreOwnerDocument::acquirePermits).sum();
          int available = this.permits() - occupied;

          // 当许可的可用数量小于申请的许可数量时，则阻塞当前线程
          if (available < permits) {
            if (identityHashCode(
                    varHandle.compareAndExchangeRelease(
                        this, currState, new StatefulVar<>(occupied)))
                == currStateHashCode) {
              return ok();
            }
            return retryableError();
          }
          Optional<SemaphoreOwnerDocument> optional =
              sd.owners().stream().filter(o -> o.equals(thisOwner)).findFirst();
          if (optional.isPresent()) {
            // 当前Thread已获得的许可数量
            int acquired = optional.get().acquirePermits();
            if ((acquired + permits) > this.permits())
              return thrownAnError("Maximum permit count exceeded");
          }

          long version = sd.version(), newVersion = version + 1;
          var filter =
              optional.isPresent()
                  ? and(
                      eq("_id", key),
                      eq("version", version),
                      elemMatch("owners", buildOwnerFilter(thisOwner)))
                  : and(eq("_id", this.getKey()), eq("version", version));
          var updates =
              optional
                  .map(
                      value ->
                          combine(
                              set("owners.$.acquire_permits", value.acquirePermits() + permits),
                              inc("version", 1)))
                  .orElseGet(() -> combine(addToSet("owners", thisOwner), inc("version", 1L)));

          return ((sd = collection.findOneAndUpdate(session, filter, updates, UPDATE_OPTIONS))
                      != null
                  && sd.containOwner(thisOwner)
                  && sd.version() == newVersion)
              ? ok()
              : retryableError();
        };

    long s = nanoTime(), waitTimeNanos = timeUnit.toNanos(waitTime);
    boolean timed = waitTime > 0;

    for (; ; ) {

      boolean elapsed =
          parkCurrentThreadUntil(
              lock,
              notFull,
              // 当申请的许可数量大于可用的许可数量时，则需要挂起当前线程
              // 可用的许可数量 = 总许可数量 - 已占用的许可数量
              () ->
                  permits
                      >= (this.permits()
                          - ((StatefulVar<Integer>) varHandle.getAcquire(this)).value),
              timed,
              s,
              (waitTimeNanos - (nanoTime() - s)));
      if (!elapsed) throw new SignalException("Timeout.");

      checkState();

      CommonTxnResponse rsp =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                  NoSuchTransaction, DuplicateKey, LockFailed),
              null,
              t -> !t.txnOk && t.retryable && !t.thrownError,
              timed,
              timed ? (waitTimeNanos - (nanoTime() - s)) : -1L,
              NANOSECONDS);

      if (rsp.txnOk) {
        doUnparkSuccessor();
        return;
      }

      // Unexpected error
      if (rsp.thrownError) throw new SignalException(rsp.message);

      // Timeout
      if (timed && (nanoTime() - s) >= waitTimeNanos) throw new SignalException("Timeout.");

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
      if (rsp.parkThread) Thread.onSpinWait();
    }
  }

  @Override
  public void release(int permits) {
    checkState();
    checkArgument(
        permits <= this.permits(),
        String.format(
            "The requested permits [%d] exceed the limit [%d].", permits, this.permits()));

    SemaphoreOwnerDocument thisOwner = buildCurrentOwner(-1);

    BiFunction<ClientSession, MongoCollection<SemaphoreDocument>, CommonTxnResponse> command =
        (session, coll) -> {
          SemaphoreDocument sd = coll.find(session, eq("_id", this.getKey())).first();

          Optional<SemaphoreOwnerDocument> thatOwnerOption;

          if (sd == null
              || sd.permits() != this.permits
              || (thatOwnerOption = sd.getThatOwner(thisOwner)).isEmpty())
            return thrownAnError(
                "Semaphore not exists or permits inconsistency or current thread does not hold a permits.");

          int acquired = thatOwnerOption.get().acquirePermits();
          int unreleased = acquired - permits;
          if (unreleased < 0)
            return thrownAnError(
                String.format(
                    "The current thread holds %d permits and is not allowed to release %d permits",
                    acquired, permits));

          long version = sd.version(), newVersion = version + 1;
          var filter = and(eq("_id", this.getKey()), eq("version", version));

          // 达到删除的条件
          if (unreleased == 0L && sd.ownerCount() == 1) {
            DeleteResult deleteResult = coll.deleteOne(session, filter);
            return deleteResult.getDeletedCount() == 1L ? ok() : retryableError();
          }
          var update =
              unreleased > 0
                  ? combine(inc("version", 1), set("owners.$.acquire_permits", unreleased))
                  : combine(inc("version", 1), pull("owners", buildOwnerFilter(thisOwner)));
          return ((sd = coll.findOneAndUpdate(session, filter, update, UPDATE_OPTIONS)) != null
                  && sd.version() == newVersion)
              ? ok()
              : retryableError();
        };

    Predicate<CommonTxnResponse> resRetryablePolicy =
        txnResult -> !txnResult.txnOk && txnResult.retryable;

    CommonTxnResponse rsp =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(NoSuchTransaction, WriteConflict),
            null,
            resRetryablePolicy);

    if (rsp.thrownError) throw new SignalException(rsp.message);
  }

  public void forceReleaseAll() {
    BiFunction<ClientSession, MongoCollection<SemaphoreDocument>, Boolean> command =
        (session, coll) -> {
          SemaphoreDocument sd = coll.find(session, eq("_id", this.getKey())).first();
          if (sd == null || this.permits() != sd.permits()) return true;

          List<SemaphoreOwnerDocument> thisOwners = sd.owners();
          if (thisOwners.stream().noneMatch(t -> this.getLease().getLeaseID().equals(t.lease()))) {
            return true;
          }
          long version = sd.version(), newVersion = version + 1L;
          var filter =
              and(
                  eq("_id", this.getKey()),
                  eq("version", sd.version()),
                  eq("permits", this.permits()));
          if (thisOwners.isEmpty()
              || thisOwners.stream().allMatch(t -> t.lease().equals(getLease().getLeaseID()))) {
            DeleteResult deleteResult = coll.deleteOne(session, filter);
            return deleteResult.getDeletedCount() == 1L;
          }
          var update =
              combine(pull("owners", eq("lease", getLease().getLeaseID())), inc("version", 1));
          return (sd = coll.findOneAndUpdate(session, filter, update)) != null
              && sd.version() == newVersion;
        };
    for (; ; ) {
      if (commandExecutor.loopExecute(
          command,
          commandExecutor.defaultDBErrorHandlePolicy(NoSuchTransaction, WriteConflict),
          null,
          t -> !t)) break;
    }
  }

  private void doUnparkSuccessor() {
    lock.lock();
    try {
      // 只唤醒头部节点
      if (((StatefulVar<Integer>) this.varHandle.getAcquire(this)).value < permits()) {
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
  @DoNotCall
  @Subscribe
  final void awakeSuccessor(ChangeStreamEvents.SemaphoreChangeAndRemovedEvent event) {
    if (!this.getKey().equals(event.semaphoreKey()) || this.permits() != event.permits()) return;

    Next:
    for (; ; ) {
      StatefulVar<Integer> currState = (StatefulVar<Integer>) varHandle.getAcquire(this);
      int currStateHashCode = identityHashCode(currState);

      // 代表删除操作
      if (event.fullDocument() == null) {
        if (identityHashCode(
                varHandle.compareAndExchangeRelease(this, currState, new StatefulVar<>(0)))
            == currStateHashCode) {
          this.doUnparkSuccessor();
          return;
        }
        Thread.onSpinWait();
        continue Next;
      }
      int occupiedPermits =
          event.fullDocument().getList("owners", Document.class).stream()
              .mapToInt(t -> t.getInteger("acquire_permits"))
              .sum();
      if (identityHashCode(
              varHandle.compareAndExchangeRelease(
                  this, currState, new StatefulVar<>(occupiedPermits)))
          == currStateHashCode) {
        this.doUnparkSuccessor();
        break Next;
      }
      Thread.onSpinWait();
      continue Next;
    }
  }

  @Override
  public int permits() {
    return permits;
  }

  @Override
  public boolean allowReleaseWithoutAcquire() {
    return allowReleaseWithoutAcquire;
  }

  @Override
  protected void doClose() {
    this.eventBus.unregister(this);
    this.forceReleaseAll();
    Utils.unparkSuccessor(lock, notFull, true);
  }

  @Override
  public <P> Collection<P> getPartnerNum() {
    BiFunction<ClientSession, MongoCollection<SemaphoreDocument>, List<SemaphoreOwnerDocument>>
        command =
            (session, coll) -> {
              var filter = eq("_id", getKey());
              SemaphoreDocument document = coll.find(filter).limit(1).first();
              return document == null ? Collections.emptyList() : document.owners();
            };
    return (Collection<P>)
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(
                LockBusy, LockFailed, LockTimeout, NoSuchTransaction),
            null,
            t -> false);
  }
}
