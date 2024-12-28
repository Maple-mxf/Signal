package signal.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class AtomicTest {
  AtomicInteger atomic = new AtomicInteger(0);
  volatile int state = 0;
  private final ExecutorService executorService = Executors.newFixedThreadPool(32);

  public static class Data implements Comparable<Data> {
    public int s;
    public long nanoTimes;

    @Override
    public int compareTo(Data o) {
      return Long.compare(o.nanoTimes, this.nanoTimes);
    }
  }

  // 5 -  969772862524916
  // 10 - 969772862523000
  @Test
  public void testCasInt() throws InterruptedException, ExecutionException {
    List<Data> queue = new CopyOnWriteArrayList<>();

    List<Future<?>> fs = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      Future<?> future =
          executorService.submit(
              () -> {
                try {
                  TimeUnit.SECONDS.sleep(1L);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
                for (; ; ) {
                  int currV = atomic.get();
                  int s = state + 1;
                  int newValue = currV + 1;
                  if (atomic.compareAndSet(currV, newValue)) {
                    state = s;
                    long nanoTime = System.nanoTime();
                    Data data = new Data();
                    data.s = newValue;
                    data.nanoTimes = nanoTime;
                    queue.add(data);
                    break;
                  }
                }
              });
      fs.add(future);
    }

    for (Future<?> f : fs) {
      f.get();
    }

    queue.stream()
        .sorted()
        .forEach(
            t -> {
              System.err.printf("%d - %d %n", t.s, t.nanoTimes);
            });

    executorService.shutdownNow();
  }

  public void testCasSecurety() {}
}
