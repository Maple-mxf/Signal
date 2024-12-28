package signal.mongo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import signal.api.Lease;

public class DistributeMutexLockTest extends BaseResourceSetup {

  private static final Logger LOGGER = LoggerFactory.getLogger(DistributeMutexLockTest.class);

  private ExecutorService executorService;

  @Override
  public void doSetup() {
    executorService = Executors.newFixedThreadPool(10);
  }

  @Override
  public void doCloseResource() {
    executorService.shutdownNow();
  }

  @Test
  public void testSmoke() throws InterruptedException {

    Lease lease = signalClient.grantLease(null);

    DistributeMutexLockImp mutexLock = (DistributeMutexLockImp) lease.getMutexLock("Test-Mutex");

    Runnable task =
        () -> {
          try {
            mutexLock.lock();
            LOGGER.debug("Thread {} lock success.", Thread.currentThread().getId());
            mutexLock.unlock();
            LOGGER.debug("Thread {} unlock success.", Thread.currentThread().getId());
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        };

    for (int i = 0; i < 10; i++) {
      Future<?> future = executorService.submit(task);
    }
    TimeUnit.SECONDS.sleep(2L);
    System.err.println(mutexLock.stateVars);
  }
}
