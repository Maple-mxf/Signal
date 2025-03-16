package signal.mongo;

import static com.google.common.collect.Lists.transform;
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
import static com.mongodb.client.model.changestream.FullDocumentBeforeChange.WHEN_AVAILABLE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.StreamSupport.stream;
import static signal.mongo.CollectionNamed.BARRIER_NAMED;
import static signal.mongo.CollectionNamed.COUNT_DOWN_LATCH_NAMED;
import static signal.mongo.CollectionNamed.DOUBLE_BARRIER_NAMED;
import static signal.mongo.CollectionNamed.LEASE_NAMED;
import static signal.mongo.CollectionNamed.MUTEX_LOCK_NAMED;
import static signal.mongo.CollectionNamed.READ_WRITE_LOCK_NAMED;
import static signal.mongo.CollectionNamed.SEMAPHORE_NAMED;
import static signal.mongo.Utils.isBitSet;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Keep;
import com.google.errorprone.annotations.ThreadSafe;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
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
import java.util.function.Consumer;
import java.util.function.Function;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.Document;
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
  private final ReentrantReadWriteLock leaseWrapperRWLock;
  private final ExecutorService executorService;

  // 代表删除操作 第0位 == 1代表删除操作
  private static final int DELETE_OP = 0b001;
  // 代表更新操作 第1位 == 1代表修改操作
  private static final int UPDATE_OP = 0b010;
  // 代表插入操作 第2位 == 1代表插入操作
  private static final int INSERT_OP = 0b100;

  private static final ImmutableMap<OperationType, Integer> OP_MAPPING =
      ImmutableMap.of(
          OperationType.INSERT, INSERT_OP,
          OperationType.UPDATE, UPDATE_OP,
          OperationType.DELETE, DELETE_OP);

  record ChangeEventsHandler(
      String signalNamed, int ops, Consumer<ChangeStreamDocument<Document>> handler) {}

  private final ChangeEventsHandler[] changeEventsDispatcher;

  private final MongoClient mongoClient;

  @SuppressWarnings("FutureReturnValueIgnored")
  private MongoSignalClient(MongoClient mongoClient, String dbNamed) {
    this.mongoClient = mongoClient;
    this.db = mongoClient.getDatabase(dbNamed);
    this.modCollDefinitions();

    this.leaseWrapperList = new ArrayList<>(8);
    this.leaseWrapperRWLock = new ReentrantReadWriteLock(true);
    this.changeEventsDispatcher = initChangeEventsDispatcher();

    this.executorService = Executors.newFixedThreadPool(8);
    this.executorService.submit(new ChangeStreamWatcher());
    this.timer = new Timer();
    this.scheduleTimeToLiveTask();
    this.scheduleClearExpireLeaseTask();
  }

  private static MongoSignalClient INSTANCE;

  /**
   * Get Single Instance
   *
   * @param client client
   * @param dbNamed dbNamed
   * @return MongoSignalClient instance
   */
  public static synchronized MongoSignalClient getInstance(MongoClient client, String dbNamed) {
    if (INSTANCE != null) return INSTANCE;
    INSTANCE = new MongoSignalClient(client, dbNamed);
    return INSTANCE;
  }

  @VisibleForTesting
  @Nullable
  ChangeEventsHandler getDispatcherHandler(String signalNamed, int op) {
    for (ChangeEventsHandler handler : changeEventsDispatcher) {
      if (!handler.signalNamed.equals(signalNamed)) continue;
      if ((DELETE_OP == op && isBitSet(handler.ops, 0))
          || (UPDATE_OP == op && isBitSet(handler.ops, 1))
          || (INSERT_OP == op && isBitSet(handler.ops, 2))) {
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
    Function<ChangeStreamDocument<Document>, String> getKeyFn =
        streamDocument ->
            ofNullable(streamDocument.getDocumentKey())
                .map(t -> t.getString("_id"))
                .map(BsonString::getValue)
                .orElse(null);

    Function<Document, Collection<String>> listLeaseIdFn =
        fullDocument ->
            ofNullable(fullDocument)
                .map(t -> t.getList("o", Document.class))
                .map(t -> t.stream().map(e -> e.getString("lease")).toList())
                .orElse(emptyList());

    BiConsumer<Object, Collection<String>> postSignalScopedEventFn =
        (scopedEvent, leaseIdList) -> {
          if (leaseIdList.isEmpty()) return;
          leaseWrapperRWLock.readLock().lock();
          try {
            leaseWrapperList.stream()
                .filter(w -> leaseIdList.contains(w.lease.getLeaseID()))
                .forEach(w -> w.lease.postScopedEvent(scopedEvent));
          } finally {
            leaseWrapperRWLock.readLock().unlock();
          }
        };

    BiConsumer<ChangeStreamDocument<Document>, Function<ChangeStreamDocument<Document>, Object>>
        eventHandlerFn =
            (streamDocument, eventProducer) -> {
              List<String> leaseIdList = new ArrayList<>();
              leaseIdList.addAll(listLeaseIdFn.apply(streamDocument.getFullDocument()));
              leaseIdList.addAll(listLeaseIdFn.apply(streamDocument.getFullDocumentBeforeChange()));
              if (leaseIdList.isEmpty()) return;

              Object scopedEvent = eventProducer.apply(streamDocument);
              postSignalScopedEventFn.accept(scopedEvent, leaseIdList);
            };

    return new ChangeEventsHandler[] {

      // 监听Lease的删除事件
      new ChangeEventsHandler(
          LEASE_NAMED,
          DELETE_OP,
          streamDocument -> {
            String leaseID = getKeyFn.apply(streamDocument);
            if (leaseID == null) {
              LOGGER.error("Document key nil.");
              return;
            }
            removeLeaseWrapper(leaseID);
          }),

      // 监听barrier的删除事件
      new ChangeEventsHandler(
          BARRIER_NAMED,
          DELETE_OP,
          streamDocument ->
              eventHandlerFn.accept(
                  streamDocument,
                  t ->
                      new ChangeStreamEvents.BarrierRemovedEvent(
                          getKeyFn.apply(t), t.getFullDocumentBeforeChange()))),

      // 监听double barrier的修改事件
      new ChangeEventsHandler(
          DOUBLE_BARRIER_NAMED,
          UPDATE_OP,
          streamDocument ->
              eventHandlerFn.accept(
                  streamDocument,
                  t -> {
                    int p =
                        streamDocument.getFullDocument() == null
                            ? t.getFullDocumentBeforeChange().getInteger("p")
                            : t.getFullDocument().getInteger("p");
                    ChangeStreamEvents.DoubleBarrierChangeEvent event =
                        new ChangeStreamEvents.DoubleBarrierChangeEvent(
                            t.getDocumentKey().getString("_id").getValue(), p, t.getFullDocument());
                    return event;
                  })),

      // 监听countdownlatch的修改事件
      new ChangeEventsHandler(
          COUNT_DOWN_LATCH_NAMED,
          UPDATE_OP,
          streamDocument -> {
            String countDownLatchKey = getKeyFn.apply(streamDocument);
            eventHandlerFn.accept(
                streamDocument,
                t ->
                    new ChangeStreamEvents.CountDownLatchChangeEvent(
                        countDownLatchKey,
                        t.getFullDocument().getInteger("c"),
                        t.getFullDocument().getInteger("cc")));
          }),

      // 监听semaphore的修改和删除事件
      new ChangeEventsHandler(
          SEMAPHORE_NAMED,
          UPDATE_OP | DELETE_OP,
          streamDocument ->
              eventHandlerFn.accept(
                  streamDocument,
                  t ->
                      new ChangeStreamEvents.SemaphoreChangeAndRemovedEvent(
                          getKeyFn.apply(t),
                          t.getFullDocument() == null
                              ? t.getFullDocumentBeforeChange().getInteger("p")
                              : t.getFullDocument().getInteger("p"),
                          t.getFullDocument()))),

      // 监听read write lock的插入、修改、删除事件
      new ChangeEventsHandler(
          READ_WRITE_LOCK_NAMED,
          INSERT_OP | UPDATE_OP | DELETE_OP,
          streamDocument ->
              eventHandlerFn.accept(
                  streamDocument,
                  t ->
                      new ChangeStreamEvents.ReadWriteLockChangeAndRemovedEvent(
                          t.getDocumentKey().getString("_id").getValue(),
                          streamDocument.getFullDocument()))),

      // 监听mutex的删除或者修改事件
      new ChangeEventsHandler(
          MUTEX_LOCK_NAMED,
          DELETE_OP | UPDATE_OP,
          streamDocument ->
              eventHandlerFn.accept(
                  streamDocument,
                  t ->
                      new ChangeStreamEvents.MutexLockChangeAndRemoveEvent(
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
              .fullDocumentBeforeChange(WHEN_AVAILABLE)
              .maxAwaitTime(50L, MILLISECONDS);
      for (ChangeStreamDocument<Document> streamDocument : changeStream) {
        MongoNamespace namespace = streamDocument.getNamespace();
        if (namespace == null) continue;

        Integer opCode;
        ChangeEventsHandler handler;
        if ((opCode = OP_MAPPING.get(streamDocument.getOperationType())) == null
            || (handler = getDispatcherHandler(namespace.getCollectionName(), opCode)) == null)
          continue;
        try {
          handler.handler.accept(streamDocument);
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

    leaseWrapperRWLock.writeLock().lock();
    try {
      leaseWrapperList.add(new LeaseWrapper(lease, lease.getCreatedTime().plusSeconds(16)));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Grant lease success. Lease id = '{}'. ", id);
      }
      return lease;
    } finally {
      leaseWrapperRWLock.writeLock().unlock();
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void scheduleTimeToLiveTask() {
    timer.schedule(new TimeToLiveTask(), 0L, 16L);
  }

  private void scheduleClearExpireLeaseTask() {
    //    CleanExpireSignalTask task = new CleanExpireSignalTask();
    //    task.run();
    //    timer.schedule(task, 1000L * 30, 1000L * 60L);
  }

  @Override
  public void close() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Closing client resource.");
    }
    leaseWrapperRWLock.writeLock().lock();
    try {
      for (LeaseWrapper w : this.leaseWrapperList) {
        if (w.lease.isRevoked()) continue;
        w.lease.revoke();
      }
      timer.cancel();
      this.executorService.shutdownNow();
      this.leaseWrapperList.clear();
    } finally {
      leaseWrapperRWLock.writeLock().unlock();
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
                          eq("_id", t.lease.getLeaseID()), set("expireAt", now.plusSeconds(32L))))
              .toList();
      BulkWriteResult bulkWriteResult =
          db.getCollection(LEASE_NAMED)
              .bulkWrite(bulkUpdates, new BulkWriteOptions().ordered(false));

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "TTL Lease success. LeaseIdList = [ {} ]. bulkWriteResult = {} ",
            ttlLeaseList.stream().map(t -> t.lease.getLeaseID()).collect(joining(",")),
            bulkWriteResult);
      }
      for (LeaseWrapper wrapper : ttlLeaseList) wrapper.nextTTLTime = now.plusSeconds(16);
    }
  }

  @Keep
  private class CleanExpireSignalTask extends TimerTask {
    @Override
    public void run() {
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
              leaseWrappers.stream()
                  .filter(t -> t.lease.getLeaseID().equals(bind.leaseID))
                  .findFirst();
          if (optional.isEmpty()) continue;

          LOGGER.debug("Expire lease[ {} ]", optional.get().lease.getLeaseID());
          leaseWrapperRWLock.writeLock().lock();

          try {
            leaseWrapperList.remove(optional.get());
          } finally {
            leaseWrapperRWLock.writeLock().unlock();
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

  private void removeLeaseWrapper(String leaseID) {
    var leaseWrappers = new ArrayList<>(this.leaseWrapperList);
    Optional<LeaseWrapper> optional =
        leaseWrappers.stream().filter(t -> t.lease.getLeaseID().equals(leaseID)).findFirst();
    if (optional.isEmpty()) return;

    leaseWrapperRWLock.writeLock().lock();
    try {
      leaseWrapperList.remove(optional.get());
    } finally {
      leaseWrapperRWLock.writeLock().unlock();
    }
  }

  private synchronized void modCollDefinitions() {
    Map<String, Document> presentCollections =
        stream(db.listCollections().spliterator(), false)
            .collect(toMap(t -> t.getString("name"), t -> t.get("options", Document.class)));

    Consumer<String> modCollOptionsFn =
        collNamed -> {
          if (!presentCollections.containsKey(collNamed)) {
            db.createCollection(
                collNamed,
                new CreateCollectionOptions()
                    .changeStreamPreAndPostImagesOptions(
                        new ChangeStreamPreAndPostImagesOptions(true)));
            return;
          }
          boolean enableChangeStreamPreAndPostImages =
              ofNullable(presentCollections.get(collNamed))
                  .map(t -> t.get("changeStreamPreAndPostImages", Document.class))
                  .map(t -> t.getBoolean("enabled", false))
                  .orElse(false);
          if (!enableChangeStreamPreAndPostImages) {
            db.runCommand(
                new Document("collMod", collNamed)
                    .append("changeStreamPreAndPostImages", new Document("enabled", true)));
          }
        };

    BiConsumer<String, Long> createTTLIndexFn =
        (collNamed, expireSecond) -> {
          MongoCollection<Document> collection = db.getCollection(collNamed);
          collection.createIndex(
              Indexes.ascending("expireAt"),
              new IndexOptions().expireAfter(expireSecond, SECONDS).name("_expireAt_"));
        };

    if (presentCollections.containsKey(LEASE_NAMED)) {
      MongoCollection<Document> collection = db.getCollection(LEASE_NAMED);
      List<Document> indexDocList = stream(collection.listIndexes().spliterator(), false).toList();
      boolean indexPresent =
          indexDocList.stream()
              .anyMatch(
                  t -> {
                    Document key = t.get("key", Document.class);
                    return key.containsKey("expireAt") && t.containsKey("expireAfterSeconds");
                  });
      if (!indexPresent) createTTLIndexFn.accept(LEASE_NAMED, 10L);
    } else {
      createTTLIndexFn.accept(LEASE_NAMED, 10L);
    }
    var signalNamedList =
        new String[] {
          LEASE_NAMED,
          BARRIER_NAMED,
          DOUBLE_BARRIER_NAMED,
          COUNT_DOWN_LATCH_NAMED,
          MUTEX_LOCK_NAMED,
          READ_WRITE_LOCK_NAMED,
          SEMAPHORE_NAMED
        };
    for (String signalCollNamed : signalNamedList) modCollOptionsFn.accept(signalCollNamed);
  }
}
