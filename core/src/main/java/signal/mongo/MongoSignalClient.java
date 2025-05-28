package signal.mongo;

import static com.google.common.collect.Lists.transform;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.size;
import static com.mongodb.client.model.Filters.type;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Updates.pullByFilter;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.changestream.FullDocument.UPDATE_LOOKUP;
import static com.mongodb.client.model.changestream.FullDocumentBeforeChange.OFF;
import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.StreamSupport.stream;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static signal.mongo.CollectionNamed.BARRIER_NAMED;
import static signal.mongo.CollectionNamed.COUNT_DOWN_LATCH_NAMED;
import static signal.mongo.CollectionNamed.DOUBLE_BARRIER_NAMED;
import static signal.mongo.CollectionNamed.LEASE_NAMED;
import static signal.mongo.CollectionNamed.MUTEX_LOCK_NAMED;
import static signal.mongo.CollectionNamed.READ_WRITE_LOCK_NAMED;
import static signal.mongo.CollectionNamed.SEMAPHORE_NAMED;
import static signal.mongo.MongoErrorCode.NO_SUCH_TRANSACTION;
import static signal.mongo.MongoErrorCode.WRITE_CONFLICT;
import static signal.mongo.Utils.isBitSet;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Keep;
import com.google.errorprone.annotations.ThreadSafe;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.lang.Nullable;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import signal.api.Lease;
import signal.api.LeaseCreateConfig;
import signal.api.SignalClient;

/** MongoSignalClient */
@ThreadSafe
@AutoService({SignalClient.class})
public class MongoSignalClient implements SignalClient {

  private static class LeaseWrapper {
    final LeaseImp lease;
    Instant nextTTLTime;

    LeaseWrapper(LeaseImp lease, Instant nextTTLTime) {
      this.lease = lease;
      this.nextTTLTime = nextTTLTime;
    }
  }

  record ExpiredSignalCollNamedBind(String leaseID, String signalCollNamed, Object signalId) {}

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoSignalClient.class);
  private final MongoDatabase db;
  private final Timer timer;

  private final List<LeaseWrapper> leaseWrapperList;
  private final ReentrantReadWriteLock rwLocker;
  private final ExecutorService executorService;

  // 代表删除操作 第0位 == 1代表删除操作
  private static final int DELETE = 0b001;
  // 代表更新操作 第1位 == 1代表修改操作
  private static final int UPDATE = 0b010;
  // 代表插入操作 第2位 == 1代表插入操作
  private static final int INSERT = 0b100;

  private static final ImmutableMap<OperationType, Integer> OP_MAPPING =
      ImmutableMap.of(
          OperationType.INSERT, INSERT,
          OperationType.UPDATE, UPDATE,
          OperationType.DELETE, DELETE);

  record ChangeEventsHandler(
      String signalNamed, int ops, Consumer<ChangeStreamDocument<Document>> handler) {}

  private final ChangeEventsHandler[] changeEventsDispatcher;

  private final MongoClient mongoClient;

  private final Lease lease;

  @SuppressWarnings("FutureReturnValueIgnored")
  private MongoSignalClient(String connectionString) {
    ConnectionString connectionStringObj = new ConnectionString(connectionString);
    if (connectionStringObj.getDatabase() == null || connectionStringObj.getDatabase().isEmpty())
      throw new IllegalArgumentException();

    this.mongoClient =
        MongoClients.create(
            MongoClientSettings.builder()
                .applyConnectionString(connectionStringObj)
                //                .applyToConnectionPoolSettings(
                //                    builder -> builder.maxSize(16).minSize(1).maxConnecting(4))
                .codecRegistry(
                    fromRegistries(
                        getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build())))
                .build());

    this.db = mongoClient.getDatabase(connectionStringObj.getDatabase());
    this.modCollDefinitions();
    this.leaseWrapperList = new ArrayList<>(8);
    this.rwLocker = new ReentrantReadWriteLock(true);
    this.changeEventsDispatcher = initChangeEventsDispatcher();

    this.executorService = Executors.newFixedThreadPool(8);
    this.executorService.submit(new ChangeStreamWatcher());
    this.timer = new Timer();

    this.lease = this.grantLease(new LeaseCreateConfig());

    this.scheduleTimeToLiveTask();
    this.scheduleClearExpireLeaseTask();
  }

  private static MongoSignalClient INSTANCE;

  /**
   * Get Single Instance
   *
   * @param connectionString mongodb connection string
   * @return MongoSignalClient instance
   */
  public static synchronized MongoSignalClient getInstance(String connectionString) {
    if (INSTANCE != null) return INSTANCE;
    INSTANCE = new MongoSignalClient(connectionString);
    return INSTANCE;
  }

  @VisibleForTesting
  @Nullable
  ChangeEventsHandler getDispatcherHandler(String signalNamed, int op) {
    for (ChangeEventsHandler handler : changeEventsDispatcher) {
      if (!handler.signalNamed.equals(signalNamed)) continue;
      if ((DELETE == op && isBitSet(handler.ops, 0))
          || (UPDATE == op && isBitSet(handler.ops, 1))
          || (INSERT == op && isBitSet(handler.ops, 2))) {
        return handler;
      }
    }
    return null;
  }

  // 在Update场景下，com.mongodb.client.model.changestream.ChangeStreamDocument.getFullDocument空的场景是
  // 在一个事务内部，包含A和B两个操作(A和B顺序执行)
  //    A：修改 id = '123' 的数据
  //    B：删除 id = '123' 的数据
  // 以上操作会导致MongoDB ChangeStream出现ChangeStreamDocument.getFullDocument()的返回值为NULL
  // 如果一个事务中包含A和B两个写操作，如果事务运行过程中出现异常，则这里的changestream不会监听到任何变更
  private ChangeEventsHandler[] initChangeEventsDispatcher() {
    Function<ChangeStreamDocument<Document>, String> mapKeyFunc =
        streamDocument ->
            ofNullable(streamDocument.getDocumentKey())
                .map(t -> t.getString("_id"))
                .map(BsonString::getValue)
                .orElse(null);

    BiConsumer<String, Object> postSignalEventFunc =
        (key, event) -> {
          rwLocker.readLock().lock();
          try {
            leaseWrapperList.forEach(
                lw -> {
                  if (lw.lease.containSignal(key)) {
                    lw.lease.postScopedEvent(event);
                  }
                });
          } finally {
            rwLocker.readLock().unlock();
          }
        };

    BiConsumer<ChangeStreamDocument<Document>, Function<ChangeStreamDocument<Document>, Object>>
        eventHandlerFn =
            (sd, ep) -> {
              if (sd.getDocumentKey() == null) return;
              postSignalEventFunc.accept(
                  sd.getDocumentKey().getString("_id").getValue(), ep.apply(sd));
            };

    return new ChangeEventsHandler[] {

      // 监听Lease的删除事件
      new ChangeEventsHandler(
          LEASE_NAMED,
          DELETE,
          sd -> {
            if (sd.getDocumentKey() == null) return;
            removeLeaseWrapper(sd.getDocumentKey().getString("_id").getValue());
          }),

      // 监听barrier的删除事件
      new ChangeEventsHandler(
          BARRIER_NAMED,
          DELETE,
          sd ->
              eventHandlerFn.accept(
                  sd, t -> new ChangeEvents.BarrierChangeEvent(mapKeyFunc.apply(t)))),

      // 监听double barrier的修改事件
      new ChangeEventsHandler(
          DOUBLE_BARRIER_NAMED,
          UPDATE,
          sd ->
              eventHandlerFn.accept(
                  sd,
                  t ->
                      new ChangeEvents.DoubleBarrierChangeEvent(
                          t.getDocumentKey().getString("_id").getValue(), t.getFullDocument()))),

      // 监听countdownlatch的修改事件
      new ChangeEventsHandler(
          COUNT_DOWN_LATCH_NAMED,
          UPDATE | INSERT | DELETE,
          sd -> {
            String cdlKey = mapKeyFunc.apply(sd);
            eventHandlerFn.accept(
                sd,
                t ->
                    new ChangeEvents.CountDownLatchChangeEvent(
                        cdlKey,
                        t.getFullDocument() == null ? 0 : t.getFullDocument().getInteger("cc"),
                        t.getFullDocument()));
          }),

      // 监听semaphore的修改和删除事件
      new ChangeEventsHandler(
          SEMAPHORE_NAMED,
          UPDATE | DELETE,
          sd ->
              eventHandlerFn.accept(
                  sd,
                  t ->
                      new ChangeEvents.SemaphoreChangeEvent(
                          mapKeyFunc.apply(t), t.getFullDocument()))),

      // 监听read write lock的插入、修改、删除事件
      new ChangeEventsHandler(
          READ_WRITE_LOCK_NAMED,
          INSERT | UPDATE | DELETE,
          sd ->
              eventHandlerFn.accept(
                  sd,
                  t ->
                      new ChangeEvents.RWLockChangeEvent(
                          t.getDocumentKey().getString("_id").getValue(), sd.getFullDocument()))),

      // 监听mutex的删除或者修改事件
      new ChangeEventsHandler(
          MUTEX_LOCK_NAMED,
          DELETE | UPDATE,
          sd ->
              eventHandlerFn.accept(
                  sd,
                  t ->
                      new ChangeEvents.MutexLockChangeEvent(
                          t.getDocumentKey().getString("_id").getValue(), t.getFullDocument())))
    };
  }

  /**
   * 开启监听相关信号量对象的任务，监听以下对象的变更事件
   *
   * <ul>
   *   <li>{@link DistributeBarrierImp}删除事件
   *   <li>{@link DistributeDoubleBarrierImp}更新事件
   * </ul>
   *
   * <p>MongoDB 6.0之后支持设置{@link
   * ChangeStreamIterable#fullDocumentBeforeChange(FullDocumentBeforeChange)}
   *
   * <p><a href="https://www.mongodb.com/docs/manual/reference/change-events/delete/">删除事件参考</a>
   * 删除事件不会返回 {@link ChangeStreamDocument#getFullDocument()} 因为文档已经不存在 默认情况下，删除事件不会返回{@link
   * ChangeStreamDocument#getFullDocumentBeforeChange}，如果需要返回需要设置以下步骤
   *
   * <ul>
   *   <li>创建集合时指定参数 {@link MongoDatabase#createCollection(ClientSession, String,
   *       CreateCollectionOptions)} {@link
   *       CreateCollectionOptions#changeStreamPreAndPostImagesOptions(ChangeStreamPreAndPostImagesOptions)}
   *       或者修改集合参数 {@link MongoDatabase#runCommand(Bson)} <code>
   *              database.runCommand(new Document("collMod", "testCollection")
   *                  .append("changeStreamPreAndPostImages", new Document("enabled", true));
   *              </code>
   *   <li>创建ChangeStream对象时指定{@link
   *       ChangeStreamIterable#fullDocumentBeforeChange(FullDocumentBeforeChange)}为{@link
   *       FullDocumentBeforeChange#WHEN_AVAILABLE}
   * </ul>
   */
  private class ChangeStreamWatcher implements Runnable {
    @Override
    public void run() {
      ChangeStreamIterable<Document> changeStream =
          db.watch(
                  singletonList(
                      match(
                          in(
                              "ns.coll",
                              LEASE_NAMED,
                              BARRIER_NAMED,
                              DOUBLE_BARRIER_NAMED,
                              COUNT_DOWN_LATCH_NAMED,
                              MUTEX_LOCK_NAMED,
                              READ_WRITE_LOCK_NAMED,
                              SEMAPHORE_NAMED))))
              .fullDocument(UPDATE_LOOKUP)
              .fullDocumentBeforeChange(OFF)
              .showExpandedEvents(false)
              .maxAwaitTime(3000L, MILLISECONDS);

      for (ChangeStreamDocument<Document> sd : changeStream) {
        MongoNamespace ns = sd.getNamespace();
        if (ns == null) continue;

        Integer opCode;
        ChangeEventsHandler handler;
        if ((opCode = OP_MAPPING.get(sd.getOperationType())) == null
            || (handler = getDispatcherHandler(ns.getCollectionName(), opCode)) == null) continue;
        try {
          handler.handler.accept(sd);
        } catch (Throwable error) {
          LOGGER
              .atError()
              .setCause(error)
              .log("Dispatcher change stream event error. {}", error.getMessage());
        }
      }
    }
  }

  @Override
  public Lease grantLease(LeaseCreateConfig config) {
    ZonedDateTime now = Instant.now().atZone(ZoneId.of("GMT"));
    String id = now.format(DateTimeFormatter.ofPattern("ddHHmmssn"));
    LeaseImp lease = new LeaseImp(this.mongoClient, this.db, id);

    rwLocker.writeLock().lock();
    try {
      leaseWrapperList.add(new LeaseWrapper(lease, lease.getCreatedTime().plusSeconds(16)));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Grant lease success. Lease id = '{}'. ", id);
      }
      return lease;
    } finally {
      rwLocker.writeLock().unlock();
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void scheduleTimeToLiveTask() {
    timer.schedule(new TimeToLiveTask(), 0L, 16L);
  }

  private void scheduleClearExpireLeaseTask() {
    CleanExpireSignalTask task = new CleanExpireSignalTask();
    task.run();
    timer.schedule(task, 1000L * 30, 1000L * 60L);
  }

  @Override
  public void close() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Closing client resource.");
    }
    rwLocker.writeLock().lock();
    try {
      for (LeaseWrapper w : this.leaseWrapperList) {
        if (w.lease.isRevoked()) continue;
        w.lease.revoke();
      }
      timer.cancel();
      this.executorService.shutdownNow();
      this.leaseWrapperList.clear();
    } finally {
      rwLocker.writeLock().unlock();
    }
  }

  /**
   * Lease续期任务
   *
   * <p>MongoDB服务端并不支持设置时区，始终使用UTC世界标准时区, Java程序在设置Lease的时间时使用{@link Clock#systemUTC()}定时时间
   */
  @Keep
  private class TimeToLiveTask extends TimerTask {
    @Override
    public void run() {
      List<LeaseWrapper> leaseWrappers = new ArrayList<>(leaseWrapperList);
      Instant now = Instant.now(Clock.systemUTC());

      List<LeaseWrapper> ttlLeaseList =
          leaseWrappers.stream()
              .filter(w -> (now.isAfter(w.nextTTLTime) || now.equals(w.nextTTLTime)))
              .toList();
      if (ttlLeaseList.isEmpty()) return;

      List<UpdateOneModel<Document>> bulkUpdates =
          ttlLeaseList.stream()
              .map(
                  t ->
                      new UpdateOneModel<Document>(
                          eq("_id", t.lease.getId()), set("expireAt", now.plusSeconds(32L))))
              .toList();
      BulkWriteResult bulkWriteResult =
          db.getCollection(LEASE_NAMED)
              .bulkWrite(bulkUpdates, new BulkWriteOptions().ordered(false));

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "TTL Lease success. LeaseIdList = [ {} ]. bulkWriteResult = {} ",
            ttlLeaseList.stream().map(t -> t.lease.getId()).collect(joining(",")),
            bulkWriteResult);
      }
      for (LeaseWrapper wrapper : ttlLeaseList) wrapper.nextTTLTime = now.plusSeconds(16);
    }
  }

  @Keep
  private class CleanExpireSignalTask extends TimerTask {
    @Override
    public void run() {
//      DistributeMutexLock mutex = lease.getMutexLock("SignalClient-CleanExpireSignalTask");
//      try {
//        boolean locked = mutex.tryLock(10L, SECONDS);
//        if (!locked) return;
//        try {
//          doRun();
//        } finally {
////          mutex.unlock();
//        }
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }
    }

    private void doRun() {
      var exclusiveSignalCollectionNamedList = new String[] {MUTEX_LOCK_NAMED};
      var shareSignalCollectionNamedList =
          new String[] {
            READ_WRITE_LOCK_NAMED,
            SEMAPHORE_NAMED,
            COUNT_DOWN_LATCH_NAMED,
            DOUBLE_BARRIER_NAMED,
            BARRIER_NAMED
          };
      try {
        List<ExpiredSignalCollNamedBind> expiredLeaseCollNamedBinds = new ArrayList<>();
        for (String collectionNamed : exclusiveSignalCollectionNamedList)
          expiredLeaseCollNamedBinds.addAll(findExpireExclusiveSignal(collectionNamed));
        for (String collectionNamed : shareSignalCollectionNamedList)
          expiredLeaseCollNamedBinds.addAll(findExpireSharedSignal(collectionNamed));

        for (ExpiredSignalCollNamedBind bind : expiredLeaseCollNamedBinds) {

          var leaseWrappers = new ArrayList<>(leaseWrapperList);
          Optional<LeaseWrapper> optional =
              leaseWrappers.stream().filter(t -> t.lease.getId().equals(bind.leaseID)).findFirst();
          if (optional.isEmpty()) continue;

          LOGGER.debug("Expire lease[ {} ]", optional.get().lease.getId());
          rwLocker.writeLock().lock();

          try {
            leaseWrapperList.remove(optional.get());
          } finally {
            rwLocker.writeLock().unlock();
          }
        }

        Map<String, List<ExpiredSignalCollNamedBind>> expireLeaseGroup =
            expiredLeaseCollNamedBinds.stream().collect(groupingBy(t -> t.signalCollNamed));

        expireLeaseGroup.forEach(
            (collectionNamed, subExpiredLeaseCollNamedBinds) -> {
              MongoCollection<Document> collection = db.getCollection(collectionNamed);
              if (MUTEX_LOCK_NAMED.equals(collectionNamed)) {
                collection.deleteMany(
                    in("_id", transform(subExpiredLeaseCollNamedBinds, t -> t.signalId)));
              } else {
                var writeOps = new ArrayList<UpdateOneModel<Document>>();
                for (ExpiredSignalCollNamedBind subExpiredSignalCollNamedBind :
                    subExpiredLeaseCollNamedBinds) {
                  var filter =
                      and(
                          eq("_id", subExpiredSignalCollNamedBind.signalId),
                          type("o", BsonType.ARRAY));
                  var update =
                      pullByFilter(
                          new Document(
                              "o", new Document("lease", subExpiredSignalCollNamedBind.leaseID)));
                  writeOps.add(new UpdateOneModel<>(filter, update));
                }

                if (writeOps.isEmpty()) return;
                BulkWriteResult bulkWriteResult =
                    collection.bulkWrite(writeOps, new BulkWriteOptions().ordered(true));
                LOGGER.warn("Clean expire signal result {} ", bulkWriteResult);
              }
            });
      } catch (Throwable error) {
        LOGGER
            .atError()
            .setCause(error)
            .log("Clean expire signal task execute fail. {}", error.getMessage());
      }
    }
  }

  @VisibleForTesting
  Collection<ExpiredSignalCollNamedBind> findExpireExclusiveSignal(String collectionNamed) {
    List<Bson> pipeline = new ArrayList<>();
    pipeline.add(Aggregates.lookup(LEASE_NAMED, "lease", "_id", "_lease"));
    pipeline.add(match(size("_lease", 0)));
    pipeline.add(project(fields(computed("lease", "$lease"), include("_id"))));
    return doFindInvalidLease(collectionNamed, pipeline);
  }

  @VisibleForTesting
  Collection<ExpiredSignalCollNamedBind> findExpireSharedSignal(String collectionNamed) {
    List<Bson> pipeline = new ArrayList<>();
    pipeline.add(unwind("$o"));
    pipeline.add(Aggregates.lookup(LEASE_NAMED, "o.lease", "_id", "_lease"));
    pipeline.add(match(size("_lease", 0)));
    pipeline.add(project(fields(include("_id"), computed("lease", "$o.lease"))));
    return doFindInvalidLease(collectionNamed, pipeline);
  }

  private Collection<ExpiredSignalCollNamedBind> doFindInvalidLease(
      String collectionNamed, List<Bson> pipeline) {
    return stream(db.getCollection(collectionNamed).aggregate(pipeline).spliterator(), false)
        .map(
            t ->
                new ExpiredSignalCollNamedBind(t.getString("lease"), collectionNamed, t.get("_id")))
        .collect(toSet());
  }

  private void removeLeaseWrapper(String leaseId) {
    var leaseWrappers = new ArrayList<>(this.leaseWrapperList);
    Optional<LeaseWrapper> optional =
        leaseWrappers.stream().filter(t -> t.lease.getId().equals(leaseId)).findFirst();
    if (optional.isEmpty()) return;

    rwLocker.writeLock().lock();
    try {
      leaseWrapperList.remove(optional.get());
    } finally {
      rwLocker.writeLock().unlock();
    }
  }

  private synchronized void modCollDefinitions() {
    CommandExecutor<Document> commandExecutor =
        new CommandExecutor<>(this, mongoClient, db.getCollection(LEASE_NAMED));

    BiFunction<ClientSession, MongoCollection<Document>, Void> command =
        (session, coll) -> {
          List<Document> documentList = new ArrayList<>(4);
          coll.listIndexes( ).into(documentList);
          boolean indexPresent =
              documentList.stream()
                  .anyMatch(
                      t -> {
                        Document key = t.get("key", Document.class);
                        return key.containsKey("expireAt") && t.containsKey("expireAfterSeconds");
                      });
          if (indexPresent) return null;
          coll.createIndex(
              session,
              Indexes.ascending("expireAt"),
              new IndexOptions().expireAfter(10L, SECONDS).name("_expireAt_"));
          return null;
        };

    Void unused =
        commandExecutor.loopExecute(
            command,
            commandExecutor.defaultDBErrorHandlePolicy(WRITE_CONFLICT, NO_SUCH_TRANSACTION));
  }
}
