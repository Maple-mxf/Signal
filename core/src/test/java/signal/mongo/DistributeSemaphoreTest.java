package signal.mongo;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import signal.api.Lease;

public class DistributeSemaphoreTest extends BaseResourceSetup {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributeSemaphoreTest.class);
  ExecutorService executorService;

  @Override
  public void doSetup() {
    executorService = Executors.newFixedThreadPool(32);
  }

  @Override
  public void doCloseResource() {
    executorService.shutdownNow();
  }

  //  @Rule public   SystemErrRule systemErrRule = new SystemErrRule().enableLog();
  //  @Rule public   SystemOutRule systemOutRule = new SystemOutRule().enableLog();
  //  @Rule public   EnvironmentVariables envVar = new EnvironmentVariables();

  @Test
  public void testCreateSemaphore() throws InterruptedException {
    Lease lease = signalClient.grantLease(null);
    DistributeSemaphoreImp semaphore =
        (DistributeSemaphoreImp) lease.getSemaphore("Test-Semaphore1", 2);

    CountDownLatch countDownLatch = new CountDownLatch(16);

    for (int i = 0; i < 16; i++) {
      CompletableFuture.runAsync(
          () -> {
            try {
              countDownLatch.countDown();
              countDownLatch.await();
              semaphore.acquire(1, -1L, TimeUnit.SECONDS);
              LOGGER.debug("Acquire 1 permits ok. {} {}", Thread.currentThread().getId(), now());
              semaphore.release(1);
            } catch (Exception e) {
              e.printStackTrace();
            } finally {

              LOGGER.debug("Release 1 permits ok. {} {} ", Thread.currentThread().getId(), now());
            }
          },
          executorService);
    }

    TimeUnit.SECONDS.sleep(5);
    System.err.println("准备revoke lease");
    System.err.println(semaphore.statefulVar);
  }

  @Test
  public void testAcquireExpire() throws InterruptedException {
    Lease lease = signalClient.grantLease(null);
    DistributeSemaphoreImp semaphore =
        (DistributeSemaphoreImp) lease.getSemaphore("Test-Semaphore1", 2);

    CompletableFuture.runAsync(
        () -> {
          try {
            TimeUnit.SECONDS.sleep(1L);
            semaphore.acquire(2, 5L, TimeUnit.SECONDS);
            LOGGER.debug("Acquire 1 permits ok. {} {}", Thread.currentThread().getId(), now());
            TimeUnit.SECONDS.sleep(6L);
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            semaphore.release(2);
            LOGGER.debug("Release 1 permits ok. {} {} ", Thread.currentThread().getId(), now());
          }
        },
        executorService);

    CompletableFuture.runAsync(
        () -> {
          try {

            semaphore.acquire(2, 5L, TimeUnit.SECONDS);
            LOGGER.debug("2 Acquire 1 permits ok. {} {}", Thread.currentThread().getId(), now());
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            semaphore.release(2);
            LOGGER.debug("2 Release 1 permits ok. {} {} ", Thread.currentThread().getId(), now());
          }
        },
        executorService);

    TimeUnit.SECONDS.sleep(8L);
    lease.revoke();
  }
}
