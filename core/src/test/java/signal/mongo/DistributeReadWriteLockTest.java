package signal.mongo;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import signal.api.DistributeReadWriteLock;
import signal.api.Lease;

public class DistributeReadWriteLockTest extends BaseResourceSetup {

  private static final Logger LOGGER = LoggerFactory.getLogger(DistributeReadWriteLockTest.class);

  ExecutorService executorService;

  @Override
  public void doSetup() {
    executorService = Executors.newFixedThreadPool(12);
  }

  @Override
  public void doCloseResource() {
    executorService.shutdownNow();
  }

  @Test
  public void testConcurrentLock() throws InterruptedException {
    Lease lease = signalClient.grantLease(null);
    DistributeReadWriteLock rwLock = lease.getReadWriteLock("TestReadWriteLock");

    Runnable task =
        () -> {
          try {

            int sed = new Random().nextInt();
            if (sed % 2 == 0) {
              rwLock.readLock().lock();
              LOGGER.debug("Thread {} try read lock success", Thread.currentThread().getId());
              rwLock.readLock().unlock();
              LOGGER.debug("Thread {} unlock read lock success", Thread.currentThread().getId());
            } else {
              LOGGER.debug("Thread {} try write lock success", Thread.currentThread().getId());
              rwLock.readLock().lock();
              LOGGER.debug("Thread {} unlock write lock success", Thread.currentThread().getId());
              rwLock.readLock().unlock();
            }

          } catch (Throwable e) {
            e.printStackTrace();
          }
        };

    for (int i = 0; i < 20; i++) {
      executorService.submit(task);
    }
    TimeUnit.SECONDS.sleep(120L);
    lease.revoke();
  }

  @Test
  public void testReadWriteMutex() throws InterruptedException {
    Lease lease = signalClient.grantLease(null);
    DistributeReadWriteLock rwLock = lease.getReadWriteLock("TestReadWriteLock");

    CompletableFuture.runAsync(()-> {
      try {
        rwLock.readLock().lock();
        LOGGER.debug("Thread {} try start get read lock success", Thread.currentThread().getId());
        TimeUnit.SECONDS.sleep(4L);
        rwLock.readLock().unlock();
        LOGGER.debug("Thread {} try start unlock read lock success", Thread.currentThread().getId());
      } catch (Throwable e) {
        e.printStackTrace();
      }

    },executorService);

    TimeUnit.SECONDS.sleep(1L);

    CompletableFuture.runAsync(()-> {
      try {
        rwLock.writeLock().lock();
        LOGGER.debug("Thread {} try start get write lock success", Thread.currentThread().getId());
        TimeUnit.SECONDS.sleep(4L);
        rwLock.writeLock().unlock();
        LOGGER.debug("Thread {} try start unlock write lock success", Thread.currentThread().getId());
      } catch (Throwable e) {
        e.printStackTrace();
      }

    },executorService);

    TimeUnit.SECONDS.sleep(16L);

    lease.revoke();
  }
}
