package signal.mongo;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class AtomicTest {

  @Test
  public void testCasInt() {
    AtomicInteger atomic = new AtomicInteger(0);
    boolean success = atomic.compareAndSet(0, 0);
    //atomic.compareAndExchangeRelease()

    //int i = atomic.compareAndExchangeAcquire();

    System.err.println(success);
  }
}
