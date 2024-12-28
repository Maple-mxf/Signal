package signal.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Before;

public class BaseResourceSetup {
  MongoClient mongoClient;
  final String dbNamed = "signal";
  MongoSignalClient signalClient;

 
  public static String now() {
    ZonedDateTime now = Instant.now().atZone(ZoneId.of("GMT"));
    return now.format(DateTimeFormatter.ofPattern("dd:HH:mm:ss:nnn"));
  }

  @Before
  public void setup() {
    this.mongoClient = MongoClients.create("mongodb://127.0.0.1:27017");
    this.signalClient = MongoSignalClient.getInstance(mongoClient, dbNamed);
    doSetup();
  }

  @After
  public void closeResource() {
    //        signalClient.close();
    mongoClient.close();
    doCloseResource();
  }

  public void doSetup() {}

  public void doCloseResource() {}
}
