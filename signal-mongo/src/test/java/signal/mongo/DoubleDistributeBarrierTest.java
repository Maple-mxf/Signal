package signal.mongo;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import signal.api.DistributeDoubleBarrier;
import signal.api.Lease;

public class DoubleDistributeBarrierTest extends BaseResourceSetup {

  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleDistributeBarrierTest.class);

  ExecutorService executorService;

  @Override
  public void doSetup() {
    executorService = Executors.newFixedThreadPool(8);
  }

  @Override
  public void doCloseResource() {
    executorService.shutdownNow();
  }

  @Test
  public void testCreateDoubleBarrier() throws InterruptedException {
    Lease lease = signalClient.grantLease(null);
    DistributeDoubleBarrier doubleBarrier = lease.getDoubleBarrier("Test-Double-Barrier", 4);

    for (int i = 0; i < 4; i++) {
      CompletableFuture.runAsync(
          () -> {
            try {
              doubleBarrier.enter();
              LOGGER.info("DoubleBarrie enter done");

              doubleBarrier.leave();
              LOGGER.info("DoubleBarrie leave end");

            } catch (Throwable e) {
              e.printStackTrace();
            }
          },
          executorService);
    }

    TimeUnit.SECONDS.sleep(4L);
    lease.revoke();
  }
}
