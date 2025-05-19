package signal.mongo;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import signal.api.DistributeBarrier;
import signal.api.Lease;

public class DistributeBarrierTest extends BaseResourceSetup {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributeBarrierTest.class);
  private ExecutorService executorService;

  @Override
  public void doSetup() {
    executorService = Executors.newFixedThreadPool(32);
  }

  @Test
  public void testSmokeFunc() throws InterruptedException {
    Lease lease = signalClient.grantLease(null);
    DistributeBarrier barrier = lease.getBarrier("Test-Barrier");
//    barrier.setBarrier();
    int concurrency = 2;
    for (int i = 0; i < concurrency; i++) {
      CompletableFuture.runAsync(
          () -> {
            try {
              LOGGER.debug(
                  "Prepare waiton the barrier ThreadId = {}", Thread.currentThread().getId());
              barrier.await();
              LOGGER.debug("Leave the barrier ThreadId = {}", Thread.currentThread().getId());
            } catch (Throwable e) {
              e.printStackTrace();
            }
          },
          executorService);
    }
    while (concurrency != barrier.getWaiterCount()) {
      Thread.onSpinWait();
    }
    TimeUnit.SECONDS.sleep(3L);
    barrier.removeBarrier();
    LOGGER.debug("Barrier has been removed.");
    TimeUnit.SECONDS.sleep(1L);
    lease.revoke();
    // TimeUnit.SECONDS.sleep(2L);
  }
}
