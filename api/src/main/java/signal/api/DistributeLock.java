package signal.api;

import com.google.errorprone.annotations.ThreadSafe;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/** 分布式锁 */
@ThreadSafe
public interface DistributeLock extends DistributeSignal, Lock {

  @Override
  void lock();

  @Override
  void lockInterruptibly() throws InterruptedException;

  @Override
  boolean tryLock();

  @Override
  boolean tryLock(long time, TimeUnit unit) throws InterruptedException;

  /** 释放锁 */
  void unlock();

  @Override
  default Condition newCondition() {
    return this.newCondition(UUID.randomUUID().toString());
  }

  Condition newCondition(String conditionKey);

  /**
   * @return 当前锁资源是否被占用
   */
  boolean isLocked();

  /**
   * @return 当前锁资源是否被当前线程占用
   */
  boolean isHeldByCurrentThread();
}
