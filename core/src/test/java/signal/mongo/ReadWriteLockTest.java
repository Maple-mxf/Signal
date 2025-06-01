package signal.mongo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import signal.api.DistributeReadWriteLock;
import signal.mongo.pojo.RWLockOwnerDocument;

public class ReadWriteLockTest extends Base {

  private DistributeReadWriteLock distributeReadWriteLock;

  @Override
  public void doSetup() {
    this.distributeReadWriteLock = lease.getReadWriteLock("Test");
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

  private Runnable newGrabReadLockReenterTask() {
    return () -> {
      int dur = new Random().nextInt(2, 4);

      boolean locked = false;
      try {
        locked = distributeReadWriteLock.readLock().tryLock();
        TimeUnit.SECONDS.sleep(2L);
        locked = distributeReadWriteLock.readLock().tryLock();
        TimeUnit.SECONDS.sleep(dur);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        if (locked) {
          distributeReadWriteLock.readLock().unlock();
          try {
            TimeUnit.SECONDS.sleep(2L);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          distributeReadWriteLock.readLock().unlock();
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

  @Test
  public void testReenter() {
    Runnable reenterTask = newGrabReadLockReenterTask();
    reenterTask.run();
  }

  @Test
  public void testWriteLockIsLocked() throws InterruptedException {
    distributeReadWriteLock.writeLock().tryLock();
    Assert.assertTrue(distributeReadWriteLock.writeLock().isLocked());
    distributeReadWriteLock.writeLock().unlock();
  }

  @Test
  public void testReadLockIsLocked() throws InterruptedException {
    distributeReadWriteLock.readLock().tryLock();
    Assert.assertTrue(distributeReadWriteLock.readLock().isLocked());
    distributeReadWriteLock.readLock().unlock();
  }

  @Test
  public void testWriteLockIsHeldByCurrentThread() throws InterruptedException {
    distributeReadWriteLock.writeLock().tryLock();
    Assert.assertTrue(distributeReadWriteLock.writeLock().isHeldByCurrentThread());
    distributeReadWriteLock.writeLock().unlock();
  }

  @Test
  public void testReadLockIsHeldByCurrentThread() throws InterruptedException {
    distributeReadWriteLock.readLock().tryLock();
    Assert.assertTrue(distributeReadWriteLock.readLock().isHeldByCurrentThread());
    distributeReadWriteLock.readLock().unlock();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadLockGetParticipants() throws InterruptedException {
    distributeReadWriteLock.readLock().tryLock();
    Collection<RWLockOwnerDocument> participants =
        (Collection<RWLockOwnerDocument>) distributeReadWriteLock.readLock().getParticipants();
    Assert.assertTrue(
        participants.stream().allMatch(p -> p.thread().equals(Utils.getCurrentThreadName())));
    distributeReadWriteLock.readLock().unlock();
  }
}
