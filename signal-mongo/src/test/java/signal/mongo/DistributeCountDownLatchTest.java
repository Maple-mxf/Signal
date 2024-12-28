package signal.mongo;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import signal.api.DistributeCountDownLatch;
import signal.api.Lease;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DistributeCountDownLatchTest extends BaseResourceSetup {
    private final static Logger LOGGER = LoggerFactory.getLogger(DistributeCountDownLatchTest.class);
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
    public void testCreateCountDownLatch() throws InterruptedException {
        Lease lease = signalClient.grantLease(null);
        DistributeCountDownLatch countDownLatch = lease.getCountDownLatch("test-count-down-latch", 8);

        for (int i = 0; i < 8; i++) {
            CompletableFuture.runAsync(() -> {
//                try {
//                    TimeUnit.SECONDS.sleep(1L);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                countDownLatch.countDown();
                LOGGER.info("End countDown");
            }, executorService);
        }

        TimeUnit.SECONDS.sleep(2L);
        countDownLatch.await();
        LOGGER.info("End await");

        lease.revoke();
    }
}
