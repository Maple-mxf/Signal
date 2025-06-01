package signal.mongo;

import org.junit.Test;
import signal.api.DistributeMutexLock;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @see java.util.concurrent.locks.Lock
 * @see java.util.concurrent.locks.ReadWriteLock
 */
public class MutexLockTest extends Base {

  private DistributeMutexLock mutexLock;

  @Override
  public void doSetup() {
    this.mutexLock = lease.getMutexLock("TestMutexLockKey");
  }

  private Runnable newGrabLockTask() {
    return () -> {
      int dur = new Random().nextInt(2, 4);
      boolean locked = false;
      try {
        locked = mutexLock.tryLock();

        log.info(
            "Thread {} grab lock success. Now : {}  Dur: {} ",
            Utils.getCurrentThreadName(),
            System.nanoTime(),
            dur);

        TimeUnit.SECONDS.sleep(new Random().nextInt(2, 4));

      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        if (locked) {
          mutexLock.unlock();
          log.info(
              "Thread {} release lock success. Now : {} ",
              Utils.getCurrentThreadName(),
              System.nanoTime());
        }
      }
    };
  }

  @Test
  public void testMutexTryLockAndUnlock() throws InterruptedException {
    Runnable grabWriteLockTask = newGrabLockTask();
    grabWriteLockTask.run();
  }

  @Test
  public void testReadWriteLock() throws InterruptedException {
    Runnable grabWriteLockTask = newGrabLockTask();

    List<CompletableFuture<?>> fsList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      fsList.add(CompletableFuture.runAsync(grabWriteLockTask));
    }

    for (CompletableFuture<?> f : fsList) {
      f.join();
    }
  }
}
