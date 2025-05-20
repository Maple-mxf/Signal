package signal.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.pull;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.setOnInsert;
import static java.lang.System.identityHashCode;
import static signal.mongo.CollectionNamed.DOUBLE_BARRIER_NAMED;
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
import static signal.mongo.Utils.unparkSuccessor;

import com.google.auto.service.AutoService;
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
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import org.bson.Document;
import signal.api.DistributeDoubleBarrier;
import signal.api.Lease;
import signal.api.SignalException;
import signal.mongo.pojo.DoubleBarrierDocument;
import signal.mongo.pojo.DoubleBarrierPartnerDocument;
import signal.mongo.pojo.DoubleBarrierPhase;

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

// TODO 需要重写内部逻辑
@ThreadSafe
@AutoService(DistributeDoubleBarrier.class)
final class DistributeDoubleBarrierImp extends DistributeMongoSignalBase<DoubleBarrierDocument>
    implements DistributeDoubleBarrier {
  private final ReentrantLock lock;
  private final Condition entered;
  private final Condition leaved;

  /**
   * 当{@link DoubleBarrierDocument#phase()} 等于 {@link DoubleBarrierPhase#ENTER} 并且enteredCount 小于
   * {@link DistributeDoubleBarrierImp#participants()}时，挂起调用enter的线程
   */
  @Keep
  @GuardedBy("enteredCountVarHandle")
  StatefulVar<Integer> enteredCount;

  /**
   * 当{@link DoubleBarrierDocument#phase()} 等于 {@link DoubleBarrierPhase#LEAVE} 并且unenteredCount
   * 不等于0时，挂起调用leave方法的线程
   */
  @GuardedBy("unenteredCountVarHandle")
  @Keep
  StatefulVar<Integer> unenteredCount;

  private final VarHandle enteredCountVarHandle;
  private final VarHandle unenteredCountVarHandle;

  private final int participantCount;
  private final EventBus eventBus;

  // TODO 待商榷的类
  private record InitDoubleBarrierTxnResponse(
      boolean success, boolean retryable, boolean thrownError, String message) {}

  public DistributeDoubleBarrierImp(
      Lease lease,
      String key,
      MongoClient mongoClient,
      MongoDatabase db,
      int participantCount,
      EventBus eventBus) {
    super(lease, key, mongoClient, db, DOUBLE_BARRIER_NAMED);
    if (participantCount <= 0) throw new IllegalArgumentException();

    this.lock = new ReentrantLock();
    this.entered = lock.newCondition();
    this.leaved = lock.newCondition();
    this.participantCount = participantCount;

    try {
      this.enteredCountVarHandle =
          MethodHandles.lookup()
              .findVarHandle(DistributeDoubleBarrierImp.class, "enteredCount", StatefulVar.class);
      this.unenteredCountVarHandle =
          MethodHandles.lookup()
              .findVarHandle(DistributeDoubleBarrierImp.class, "unenteredCount", StatefulVar.class);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException();
    }

    BiFunction<ClientSession, MongoCollection<DoubleBarrierDocument>, CommonTxnResponse> command =
        (session, coll) -> {
          // 执行更新version的操作，保证事物读到的数据不是旧事物版本的数据
          DoubleBarrierDocument dbd =
              coll.findOneAndUpdate(
                  session,
                  eq("_id", this.getKey()),
                  combine(
                      setOnInsert("_id", key),
                      setOnInsert("partner_num", participantCount),
                      setOnInsert("partners", Collections.emptyList()),
                      setOnInsert("phase", DoubleBarrierPhase.ENTER),
                      inc("version", 1L)),
                  UPSERT_OPTIONS);
          if (dbd == null) return retryableError();
          if (dbd.partnerNum() != this.participantCount) {
            String message =
                String.format(
                    """
                                  The current number of participants is %d, which is not equal to the set number of %d.
                        """,
                    this.participantCount, dbd.partnerNum());
            return thrownAnError(message);
          }
          return ok();
        };

    for (; ; ) {
      CommonTxnResponse rsp =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                  NoSuchTransaction, WriteConflict, DuplicateKey),
              null,
              t -> !t.txnOk && t.retryable && !t.thrownError);
      if (rsp.thrownError) throw new SignalException(rsp.message);
      if (rsp.txnOk) break;
    }

    // 初始化为阻塞的状态
    enteredCount = new StatefulVar<>(0);
    unenteredCount = new StatefulVar<>(participantCount);

    this.eventBus = eventBus;
    this.eventBus.register(this);
  }

  @Override
  public int participants() {
    return participantCount;
  }

  private DoubleBarrierPartnerDocument buildCurrentPartner() {
    return new DoubleBarrierPartnerDocument(
        Utils.getCurrentHostname(), this.getLease().getLeaseID(), Utils.getCurrentThreadName());
  }

  @Override
  public void enter() throws InterruptedException {
    checkState();
    DoubleBarrierPartnerDocument thisPartner = buildCurrentPartner();
    BiFunction<ClientSession, MongoCollection<DoubleBarrierDocument>, CommonTxnResponse> command =
        (session, coll) -> {
          @SuppressWarnings("unchecked")
          StatefulVar<Integer> currState =
              (StatefulVar<Integer>) enteredCountVarHandle.getAcquire(this);
          int currStateHashCode = identityHashCode(currState);

          DoubleBarrierDocument dbd;
          if ((dbd = coll.find(session, eq("_id", this.getKey())).first()) == null) {
            dbd =
                new DoubleBarrierDocument(
                    key,
                    participantCount,
                    List.of(thisPartner),
                    participants() == 1 ? DoubleBarrierPhase.LEAVE : DoubleBarrierPhase.ENTER,
                    1L);
            InsertOneResult insertOneResult = coll.insertOne(session, dbd);
            boolean success =
                insertOneResult.getInsertedId() != null && insertOneResult.wasAcknowledged();
            if (!success) return retryableError();

            if (identityHashCode(
                    enteredCountVarHandle.compareAndExchangeRelease(
                        this, currState, new StatefulVar<>(dbd.partners().size())))
                == currStateHashCode) {
              return ok();
            }
            return retryableError();
          }

          if (dbd.partnerNum() != this.participantCount)
            return thrownAnError(
                String.format(
                    """
                                The current number of participants is %d, which is not equal to the set number of %d.
                    """,
                    this.participantCount, dbd.partnerNum()));

          if (dbd.phase() != DoubleBarrierPhase.ENTER)
            return thrownAnError("The current phase is not entering.");

          List<DoubleBarrierPartnerDocument> partners = dbd.partners();

          Optional<DoubleBarrierPartnerDocument> thatPartnerOption =
              dbd.getThatPartner(thisPartner);

          if (thatPartnerOption.isPresent()) {
            if (identityHashCode(
                    enteredCountVarHandle.compareAndExchangeRelease(
                        this, currState, new StatefulVar<>(dbd.partners().size())))
                == currStateHashCode) {
              return ok();
            }
            return retryableError();
          }

          if (partners.size() >= this.participantCount)
            return thrownAnError("The current number of participants is overflow.");

          long version = dbd.version(), newVersion = version + 1;
          boolean success =
              ((dbd =
                          coll.findOneAndUpdate(
                              session,
                              and(eq("_id", key), eq("version", version)),
                              combine(
                                  addToSet("partners", thisPartner),
                                  inc("version", 1L),
                                  set(
                                      "phase",
                                      (partners.size() + 1 == this.participantCount)
                                          ? DoubleBarrierPhase.LEAVE
                                          : DoubleBarrierPhase.ENTER)),
                              UPDATE_OPTIONS))
                      != null
                  && dbd.partners().contains(thisPartner)
                  && dbd.version() == newVersion);

          if (!success) return retryableError();
          if (identityHashCode(
                  enteredCountVarHandle.compareAndExchangeRelease(
                      this, currState, new StatefulVar<>(dbd.partners().size())))
              == currStateHashCode) {
            return ok();
          }
          return retryableError();
        };

    Next:
    for (; ; ) {
      CommonTxnResponse rsp =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(
                  NoSuchTransaction, WriteConflict, DuplicateKey),
              null,
              t -> !t.txnOk && t.retryable && !t.thrownError);
      if (rsp.thrownError) throw new SignalException(rsp.message);
      if (!rsp.txnOk && rsp.retryable) continue Next;
      parkCurrentThreadUntil(
          lock,
          entered,
          () -> {
            @SuppressWarnings("unchecked")
            StatefulVar<Integer> sv = (StatefulVar<Integer>) enteredCountVarHandle.getAcquire(this);
            return sv.value < participants();
          },
          false,
          0,
          0);
      break Next;
    }
  }

  @Override
  public void leave() throws InterruptedException {
    checkState();
    DoubleBarrierPartnerDocument thisPartner = buildCurrentPartner();

    BiFunction<ClientSession, MongoCollection<DoubleBarrierDocument>, CommonTxnResponse> command =
        (session, coll) -> {
          @SuppressWarnings("unchecked")
          StatefulVar<Integer> currState =
              (StatefulVar<Integer>) unenteredCountVarHandle.getAcquire(this);
          int currStateHashCode = identityHashCode(currState);

          DoubleBarrierDocument dbd;
          if ((dbd = coll.find(session, eq("_id", this.getKey())).first()) == null) {
            return thrownAnError("Double barrier missing.");
          }

          if (dbd.partnerNum() != this.participantCount)
            return thrownAnError(
                String.format(
                    """
                    The current number of participants is %d, which is not equal to the set number of %d.
                    """,
                    this.participantCount, dbd.partnerNum()));

          if (dbd.phase() != DoubleBarrierPhase.LEAVE)
            return thrownAnError("The current phase is not leaving.");

          List<DoubleBarrierPartnerDocument> partners = dbd.partners();
          if (partners.size() > this.participantCount)
            return thrownAnError("The current number of participants is overflow.");

          if (dbd.partners().size() == 1 && dbd.partners().contains(thisPartner)) {
            DeleteResult deleteResult =
                coll.deleteOne(session, and(eq("_id", key), eq("version", dbd.version())));
            boolean success =
                deleteResult.wasAcknowledged() && deleteResult.getDeletedCount() == 1L;

            if (!success) return retryableError();
            if (identityHashCode(
                    unenteredCountVarHandle.compareAndExchangeRelease(
                        this, currState, new StatefulVar<>(0)))
                == currStateHashCode) {
              return ok();
            }
            return retryableError();
          }
          long version = dbd.version(), newVersion = version + 1;
          boolean success =
              ((dbd =
                          coll.findOneAndUpdate(
                              session,
                              and(eq("_id", key), eq("version", version)),
                              combine(
                                  combine(
                                      pull(
                                          "partners",
                                          and(
                                              eq("lease", thisPartner.lease()),
                                              eq("thread", thisPartner.thread()),
                                              eq("hostname", thisPartner.hostname()))),
                                      inc("version", 1))),
                              UPDATE_OPTIONS))
                      != null
                  && !dbd.partners().contains(thisPartner)
                  && dbd.version() == newVersion);

          if (!success) return retryableError();
          if (identityHashCode(
                  unenteredCountVarHandle.compareAndExchangeRelease(
                      this, currState, new StatefulVar<>(dbd.partners().size())))
              == currStateHashCode) {
            return ok();
          }
          return retryableError();
        };

    Next:
    for (; ; ) {
      CommonTxnResponse rsp =
          commandExecutor.loopExecute(
              command,
              commandExecutor.defaultDBErrorHandlePolicy(NoSuchTransaction, WriteConflict),
              null,
              t -> !t.txnOk && t.retryable && !t.thrownError);
      if (rsp.thrownError) throw new SignalException(rsp.message);
      if (!rsp.txnOk && rsp.retryable) continue Next;
      parkCurrentThreadUntil(
          lock,
          leaved,
          () -> {
            @SuppressWarnings("unchecked")
            StatefulVar<Integer> sv =
                (StatefulVar<Integer>) unenteredCountVarHandle.getAcquire(this);
            return sv.value > 0;
          },
          false,
          0,
          0);
      break Next;
    }
  }

  @Override
  protected void doClose() {
    this.eventBus.unregister(this);
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
  void awakeAllSuccessor(ChangeStreamEvents.DoubleBarrierChangeEvent event) {
    if (!this.getKey().equals(event.doubleBarrierKey())
        || event.participants() != this.participants()) return;

    Next:
    for (; ; ) {
      @SuppressWarnings("unchecked")
      StatefulVar<Integer> currEnteredCountState =
          (StatefulVar<Integer>) enteredCountVarHandle.getAcquire(this);
      int currEnteredCountStateHashCode = identityHashCode(currEnteredCountState);

      @SuppressWarnings("unchecked")
      StatefulVar<Integer> currUnenteredCountState =
          (StatefulVar<Integer>) unenteredCountVarHandle.getAcquire(this);
      int currUnenteredCountStateHashCode = identityHashCode(currUnenteredCountState);

      // 代表删除操作
      Document fullDocument = event.fullDocument();
      if (fullDocument == null) {
        if (identityHashCode(
                unenteredCountVarHandle.compareAndExchangeRelease(
                    this, currUnenteredCountState, new StatefulVar<>(0)))
            == currUnenteredCountStateHashCode) {
          unparkSuccessor(lock, leaved, true);
          return;
        }
        Thread.onSpinWait();
        continue Next;
      }

      String phase = fullDocument.getString("phase");
      if (DoubleBarrierPhase.ENTER.name().equals(phase)) {
        if (fullDocument.getList("partners", Document.class).size() == participantCount) {
          if (identityHashCode(
                  enteredCountVarHandle.compareAndExchangeRelease(
                      this, currEnteredCountState, new StatefulVar<>(participantCount)))
              == currEnteredCountStateHashCode) {
            unparkSuccessor(lock, entered, true);
            break Next;
          }
          Thread.onSpinWait();
          continue Next;
        }
        break Next;
      } else if (DoubleBarrierPhase.LEAVE.name().equals(phase)) {
        if (fullDocument.getList("partners", Document.class).isEmpty()) {
          if (identityHashCode(
                  unenteredCountVarHandle.compareAndExchangeRelease(
                      this, currUnenteredCountState, new StatefulVar<>(0)))
              == currUnenteredCountStateHashCode) {
            unparkSuccessor(lock, leaved, true);
            break Next;
          }
          Thread.onSpinWait();
          continue Next;
        }
        break Next;
      }
    }
  }

  @Override
  public Collection<?> getParticipants() {
    BiFunction<
            ClientSession,
            MongoCollection<DoubleBarrierDocument>,
            List<DoubleBarrierPartnerDocument>>
        command =
            (session, coll) -> {
              var filter = eq("_id", getKey());
              DoubleBarrierDocument document = coll.find(filter).limit(1).first();
              return document == null ? Collections.emptyList() : document.partners();
            };
    return commandExecutor.loopExecute(
        command,
        commandExecutor.defaultDBErrorHandlePolicy(
            LockBusy, LockFailed, LockTimeout, NoSuchTransaction),
        null,
        t -> false);
  }
}
