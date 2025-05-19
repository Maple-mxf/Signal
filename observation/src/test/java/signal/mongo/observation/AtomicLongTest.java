package signal.mongo.observation;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

public class AtomicLongTest {

  @Test
  public void testAtomic() {
    AtomicLong count = new AtomicLong(0);

    boolean success = count.compareAndSet(0L, 1L);
    long exchange1 = count.compareAndExchange(2L, 3L);

    long exchan2 = count.compareAndExchangeAcquire(1L,2L);

    System.err.println(exchan2);
    System.err.println(count.get());
  }
}
