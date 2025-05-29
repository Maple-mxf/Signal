package signal.mongo;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import signal.api.Lease;
import signal.api.LeaseCreateConfig;

public class Base {
  MongoSignalClient signalClient;

  Lease lease;

  Logger log = LoggerFactory.getLogger("signal.mongo.Signal");

  public static String now() {
    ZonedDateTime now = Instant.now().atZone(ZoneId.of("GMT"));
    return now.format(DateTimeFormatter.ofPattern("dd:HH:mm:ss:nnn"));
  }

  @Before
  public void setup() {
    this.signalClient = MongoSignalClient.getInstance("mongodb://127.0.0.1:27017/signal");
    this.lease = signalClient.grantLease(new LeaseCreateConfig());
    doSetup();
  }

  @After
  public void closeResource() {
    doCloseResource();
    signalClient.close();
  }

  public void doSetup() {}

  public void doCloseResource() {}
}
