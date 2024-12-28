package signal.api;

import java.util.concurrent.TimeUnit;

/**
 * 分布式锁
 */
public interface DistributeLock extends DistributeSignal {

  default void lock() throws InterruptedException {
    tryLock(-1L, TimeUnit.NANOSECONDS);
  }

  boolean tryLock(Long waitTime, TimeUnit timeUnit) throws InterruptedException;

  void unlock();

  boolean isLocked();

  boolean isHeldByCurrentThread();
}
