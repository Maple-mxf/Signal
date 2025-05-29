package signal.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import signal.api.DistributeReadWriteLock;

public class ReadWriteLockTest extends Base {

  private DistributeReadWriteLock distributeReadWriteLock;

  @Override
  public void doSetup() {
    this.distributeReadWriteLock = lease.getReadWriteLock("Test");
  }

  @Override
  public void doCloseResource() {
    //    this.distributeReadWriteLock.close();
  }

  private Runnable newGrabWriteLockTask() {
    return () -> {
      int dur = new Random().nextInt(2, 4);
      boolean locked = false;
      try {
        locked = distributeReadWriteLock.writeLock().tryLock();

        log.info(
            "Thread {} grab write lock success. Now : {}  Dur: {} ",
            Utils.getCurrentThreadName(),
            System.nanoTime(),
            dur);

        TimeUnit.SECONDS.sleep(new Random().nextInt(2, 4));

      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        if (locked) {
          distributeReadWriteLock.writeLock().unlock();
          log.info(
              "Thread {} release write lock success. Now : {} ",
              Utils.getCurrentThreadName(),
              System.nanoTime());
        }
      }
    };
  }

  private Runnable newGrabReadLockTask() {
    return () -> {
      int dur = new Random().nextInt(2, 4);

      boolean locked = false;
      try {
        locked = distributeReadWriteLock.readLock().tryLock();
        log.info(
            "Thread {} grab read lock success. Now : {} dur {} ",
            Utils.getCurrentThreadName(),
            System.nanoTime(),
            dur);
        TimeUnit.SECONDS.sleep(new Random().nextInt(2, 4));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        if (locked) {
          distributeReadWriteLock.readLock().unlock();
          log.info(
              "Thread {} release read lock success. Now : {} ",
              Utils.getCurrentThreadName(),
              System.nanoTime());
        }
      }
    };
  }
  @Test
  public void testReadWriteLock2() throws InterruptedException {
    Runnable grabWriteLockTask = newGrabWriteLockTask();
    grabWriteLockTask.run();
  }
  @Test
  public void testReadWriteLock() throws InterruptedException {
    Runnable grabWriteLockTask = newGrabWriteLockTask();
    Runnable grabReadLockTask = newGrabReadLockTask();

    List<CompletableFuture<?>> fsList = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      fsList.add(CompletableFuture.runAsync(grabReadLockTask));
    }
    for (int i = 0; i < 2; i++) {
      fsList.add(CompletableFuture.runAsync(grabWriteLockTask));
    }

    for (CompletableFuture<?> f : fsList) {
      f.join();
    }
  }
}
