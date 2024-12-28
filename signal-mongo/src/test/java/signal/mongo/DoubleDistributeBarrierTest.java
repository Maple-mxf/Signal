package signal.mongo;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import signal.api.DistributeDoubleBarrier;
import signal.api.Lease;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

    List<Thread> threads = new CopyOnWriteArrayList<>();

    for (int i = 0; i < 4; i++) {
      CompletableFuture.runAsync(
          () -> {
            try {
              threads.add(Thread.currentThread());
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
    TimeUnit.SECONDS.sleep(124L);
    for (Thread thread : threads) {
      System.err.println(thread.getState());
    }

    //lease.revoke();
  }

  public static void main(String[] args){
    System.err.println(3&2);
  }
}
