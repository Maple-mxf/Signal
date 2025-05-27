package signal.api;

import com.google.errorprone.annotations.ThreadSafe;

import java.util.concurrent.TimeUnit;

/** 分布式锁 */
@ThreadSafe
public interface DistributeLock extends DistributeSignal {

  /**
   * 进行加锁尝试，无限等待，若锁资源被占用，则阻塞当前线程
   *
   * @throws InterruptedException 调用lock操作的线程进入{@link
   *     java.lang.Thread.State#WAITING}状态，如果主线程被kill，此时会抛出此异常
   */
  default boolean tryLock() throws InterruptedException, SignalException {
    return tryLock(-1L, TimeUnit.NANOSECONDS);
  }

  /**
   * @param waitTime 等待时间
   * @param timeUnit 时间单位
   * @return 如果超时，则会返回false，否则是true
   * @throws InterruptedException 调用lock操作的线程进入{@link java.lang.Thread.State#TIMED_WAITING}状态，
   *     如果主线程被kill，此时会抛出此异常
   * @throws SignalException 超时或者非预期的错误，会抛出此异常
   */
  boolean tryLock(Long waitTime, TimeUnit timeUnit) throws InterruptedException, SignalException;

  /** 释放锁 */
  void unlock();

  /**
   * @return 当前锁资源是否被占用
   */
  boolean isLocked();

  /**
   * @return 当前锁资源是否被当前线程占用
   */
  boolean isHeldByCurrentThread();
}
