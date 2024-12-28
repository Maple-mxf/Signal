package signal.api;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public interface DistributeSemaphore extends DistributeSignal {
  default void acquire() throws InterruptedException {
    this.acquire(1, -1L, TimeUnit.NANOSECONDS);
  }

  default void acquire(int permits) throws InterruptedException {
    this.acquire(permits, -1L, TimeUnit.NANOSECONDS);
  }

  default void acquire(Long waitTime, TimeUnit timeUnit) throws InterruptedException {
    this.acquire(1, waitTime, timeUnit);
  }

  void acquire(int permits, Long waitTime, TimeUnit timeUnit) throws InterruptedException;

  default void release() {
    this.release(1);
  }

  void release(int permits);

  int permits();

  Collection<Holder> getHolders();
}
