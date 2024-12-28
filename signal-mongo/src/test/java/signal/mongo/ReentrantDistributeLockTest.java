package signal.mongo;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ReentrantDistributeLockTest {

  @Test
  public void testConditions() throws InterruptedException {
    ReentrantLock lock = new ReentrantLock();
    Condition condition = lock.newCondition();

    CompletableFuture.runAsync(
        () -> {
          lock.lock();
          try {
            System.err.println("Await开始");
            boolean awaited = condition.await(3L, TimeUnit.SECONDS);
            System.out.println(awaited);
            System.err.println("Await结束");
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          lock.unlock();
        });

    CompletableFuture.runAsync(
        () -> {
          try {
            TimeUnit.SECONDS.sleep(2L);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          lock.lock();
          condition.signal();
          lock.unlock();
        });
    TimeUnit.SECONDS.sleep(4L);
  }
}
