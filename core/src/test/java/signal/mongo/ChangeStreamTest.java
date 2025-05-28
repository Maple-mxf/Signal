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
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ChangeStreamTest {

  private MongoClient client;
  private MongoDatabase db;

  @Before
  public void setup() {
    client = MongoClients.create("mongodb://127.0.0.1:5707");
    db = client.getDatabase("signal");
  }

  @After
  public void after() {
    client.close();
  }

  @Test
  public void testWatchTransactionChange2() throws InterruptedException {
    // 集合监听
    db.getCollection("student")
        .watch(Document.class)
        .fullDocument(FullDocument.UPDATE_LOOKUP)
        .showExpandedEvents(true)
        .forEach(streamDocument -> System.out.printf("%s %s   %n",
                streamDocument.getExtraElements(),
                streamDocument.toString()));
  }

  @Test
  public void testWatchTransactionChange() throws InterruptedException {

    // 集合监听
    Runnable watchTask =
        () -> {
          db.getCollection("student")
              .watch(Document.class)
              .fullDocument(FullDocument.UPDATE_LOOKUP)
              .forEach(
                  streamDocument -> {
                    System.out.printf(
                        "FullDocument = %s Time = %d %n",
                        streamDocument.getFullDocument(), System.nanoTime());
                  });
        };
    CompletableFuture.runAsync(watchTask);

    Runnable updateInTxnTask =
        () -> {
          MongoCollection<Document> coll = db.getCollection("student");
          try (ClientSession session = client.startSession()) {
            session.withTransaction(
                (TransactionBody<Boolean>)
                    () -> {
                      UpdateResult updateResult =
                          coll.updateOne(
                              session,
                              eq("_id", new ObjectId("6776301ae26fa14481fc0421")),
                              Updates.set("desc", "desc111111"));

                      UpdateResult updateResult2 =
                          coll.updateOne(
                              session,
                              eq("_id", new ObjectId("6776301ae26fa14481fc0421")),
                              Updates.set("desc", "desc222222"));

                      throw new RuntimeException("xxx");
                    },
                TRANSACTION_OPTIONS);
          }
        };
    CompletableFuture.runAsync(updateInTxnTask);
    TimeUnit.SECONDS.sleep(2L);
  }
}
