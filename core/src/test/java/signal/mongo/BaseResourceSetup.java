package signal.mongo;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Before;

public class BaseResourceSetup {
  MongoSignalClient signalClient;

  public static String now() {
    ZonedDateTime now = Instant.now().atZone(ZoneId.of("GMT"));
    return now.format(DateTimeFormatter.ofPattern("dd:HH:mm:ss:nnn"));
  }

  @Before
  public void setup() {

    this.signalClient = MongoSignalClient.getInstance("mongodb://127.0.0.1:5707/signal");
    doSetup();
  }

  @After
  public void closeResource() {
    signalClient.close();
    doCloseResource();
  }

  public void doSetup() {}

  public void doCloseResource() {}
}
