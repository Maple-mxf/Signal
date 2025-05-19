package signal.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.Before;
import org.junit.Test;

public class VolitileTest {

  private volatile int a = 0;

  private ExecutorService executorService;

  @Before
  public void setup() {
    executorService = Executors.newFixedThreadPool(10);
  }

  @Test
  public void testConcurrentModValue() throws InterruptedException, ExecutionException {
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Future<?> future =
          executorService.submit(
              () -> {
                try {
                  TimeUnit.SECONDS.sleep(1L);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              });
      futures.add(future);
    }

    for (Future<?> future : futures) {
      future.get();
      System.err.println(future.isDone());
    }

    ReentrantLock lock = new ReentrantLock();
    lock.lock();

    Condition condition = lock.newCondition();
    condition.signalAll();
    condition.await();;

    lock.unlock();

    System.err.println(a);
  }

  @Test
  public void testHashCode() {
  }
}
