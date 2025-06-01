package signal.mongo;

import static com.mongodb.client.model.Filters.eq;
import static signal.mongo.DistributeMongoSignalBase.TRANSACTION_OPTIONS;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.result.UpdateResult;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ChangeStreamTest {

  private MongoClient client;
  private MongoDatabase db;

  @Before
  public void setup() {
    client = MongoClients.create("mongodb://127.0.0.1:27017/signal");
    db = client.getDatabase("signal");
  }

  @After
  public void after() {
    client.close();
  }

  @Test
  public void testWatchTransactionChange2() throws InterruptedException {
    // 集合监听
    db.getCollection("signal_mutex_lock")
        .watch(Document.class)
        .fullDocument(FullDocument.UPDATE_LOOKUP)
        .showExpandedEvents(true)
        .forEach(
            sd -> {
              System.out.printf(
                  "OperationType %s. FullDocument %s %n",
                  sd.getOperationType(),
                  sd.getFullDocument() == null ? "" : sd.getFullDocument().toJson());
            });
  }

  //   // 在Update场景下，com.mongodb.client.model.changestream.ChangeStreamDocument.getFullDocument空的场景是
  //  // 在一个事务内部，包含A和B两个操作(A和B顺序执行)
  //  //    A：修改 id = '123' 的数据
  //  //    B：删除 id = '123' 的数据
  //  // 以上操作会导致MongoDB ChangeStream出现ChangeStreamDocument.getFullDocument()的返回值为NULL
  //  // 如果一个事务中包含A和B两个写操作，如果事务运行过程中出现异常，则这里的changestream不会监听到任何变更

  @Test
  public void testWatchTransactionChange() throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Runnable watchTask =
        () -> {
          countDownLatch.countDown();
          db.getCollection("student")
              .watch(Document.class)
              .showExpandedEvents(true)
              .forEach(sd -> System.out.printf("  SD = %s   %n", sd));
        };
    CompletableFuture.runAsync(watchTask);
    countDownLatch.await();
    Runnable updateInTxnTask =
        () -> {
          MongoCollection<Document> coll = db.getCollection("student");
          try (ClientSession session = client.startSession()) {
            session.withTransaction(
                (TransactionBody<Boolean>)
                    () -> {
                      UpdateResult updateResult =
                          coll.updateOne(
                              session, eq("_id", 3), Updates.set("name", "desc1112222111"));
                      UpdateResult updateResult2 =
                          coll.updateOne(
                              session, eq("_id", 3), Updates.set("name", "desc22222233333"));
                      coll.deleteOne(session, eq("_id", 3));
                      return true;
                    },
                TRANSACTION_OPTIONS);
          } finally {
            System.err.println("事务结束");
          }
        };
    CompletableFuture.runAsync(updateInTxnTask);
    TimeUnit.SECONDS.sleep(10L);
  }
}
