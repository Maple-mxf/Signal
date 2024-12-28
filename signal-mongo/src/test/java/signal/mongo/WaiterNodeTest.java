package signal.mongo;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class WaiterNodeTest {

  @Test
  public void testTimeout() throws InterruptedException {

    ReentrantLock lock = new ReentrantLock();
    Condition condition = lock.newCondition();

    CompletableFuture.runAsync(
        () -> {
          try {
            lock.lock();
            System.err.println(lock.isLocked());
            TimeUnit.SECONDS.sleep(3L);
                        condition.await();
            System.err.println("wait 结束 + " + lock.isLocked());
            lock.unlock();
            System.err.println("unlock");
          } catch (Exception e) {
            e.printStackTrace();
          }
        });

    CompletableFuture.runAsync(
        () -> {
          try {
            TimeUnit.SECONDS.sleep(2L);
            lock.lock();
            System.err.println("xx" + lock.isLocked());
            condition.signal();
            lock.unlock();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    TimeUnit.SECONDS.sleep(4L);
  }
}
