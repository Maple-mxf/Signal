package signal.mongo;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class JavaDistributeSemaphoreTest {

  // Fair
  //         protected int tryAcquireShared(int acquires) {
  //            for (;;) {
  //                if (hasQueuedPredecessors())
  //                    return -1;
  //                int available = getState();
  //                int remaining = available - acquires;
  //                if (remaining < 0 ||
  //                    compareAndSetState(available, remaining))
  //                    return remaining;
  //            }
  //        }

  // NotFair
  // final int nonfairTryAcquireShared(int acquires) {
  //            for (;;) {
  //                int available = getState();
  //                int remaining = available - acquires;
  //                if (remaining < 0 ||
  //                    compareAndSetState(available, remaining))
  //                    return remaining;
  //            }
  //        }
  @Test
  public void testSemaphore() throws InterruptedException {
    Semaphore semaphore = new Semaphore(2, true);
    for (int i = 0; i < 16; i++) {
      CompletableFuture.runAsync(
          () -> {
            try {
              semaphore.acquire();
              System.err.println("acquire OK .");
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            } finally {
              semaphore.release();
              System.err.println("release OK .");
            }
          });
    }
    TimeUnit.SECONDS.sleep(10L);
  }
}
