package signal.api;


import com.google.errorprone.annotations.ThreadSafe;

import java.util.concurrent.TimeUnit;

/**
 * 信号量维护一个整数计数值，这个值表示可用资源的数量或许可证数。
 *
 * <p>信号量支持两个基本操作
 *
 * <ul>
 *   <li>
 *       <p>P（Proberen） 或 acquire()（减操作）
 *       <ul>
 *         <li>线程请求一个许可证（即，尝试获取资源）。
 *         <li>如果信号量的计数大于 0，线程可以成功获取一个许可证，信号量的计数减 1。
 *         <li>如果信号量的计数为 0，线程将被阻塞，直到有其他线程释放一个许可证。
 *       </ul>
 *   <li>
 *       <p>V（Verhogen） 或 release()（加操作）：
 *       <ul>
 *         <li>线程释放一个许可证（即，释放资源）
 *         <li>信号量的计数加 1，表示一个资源变得可用
 *         <li>如果有其他线程在等待许可证，则被唤醒并允许继续执行
 *       </ul>
 * </ul>
 *
 * <p>信号量内部会维护一个队列存储等待的线程信息存储阻塞等待的线程信息
 */
@ThreadSafe
public interface DistributeSemaphore extends DistributeSignal, Shared {

  /**
   * 申请1个许可，如果许可资源不足，则阻塞当前执行线程
   *
   * @throws InterruptedException 主线程deadline，wait状态的线程会抛出此错误
   */
  default void acquire() throws InterruptedException {
    this.acquire(1, -1L, TimeUnit.NANOSECONDS);
  }

  /**
   * 申请一个或者多个许可，如果许可资源不足，则阻塞当前执行线程
   *
   * @param permits 申请的许可数量
   * @throws InterruptedException 主线程deadline，wait状态的线程会抛出此错误
   */
  default void acquire(int permits) throws InterruptedException {
    this.acquire(permits, -1L, TimeUnit.NANOSECONDS);
  }

  /**
   * 申请一个许可，如果许可资源不足，则阻塞当前执行线程
   *
   * @param waitTime 等待的事件
   * @param timeUnit 时间单位
   * @throws InterruptedException 主线程deadline，wait状态的线程会抛出此错误
   * @throws SignalException 当等待超时抛出Timeout错误
   */
  default void acquire(Long waitTime, TimeUnit timeUnit) throws InterruptedException {
    this.acquire(1, waitTime, timeUnit);
  }

  /**
   * 申请一个或者多个许可，如果许可资源不足，则阻塞当前执行线程
   *
   * @param permits 许可数量
   * @param waitTime 等待的事件
   * @param timeUnit 时间单位
   * @throws InterruptedException 主线程deadline，wait状态的线程会抛出此错误
   * @throws SignalException 当等待超时抛出Timeout错误
   */
  void acquire(int permits, Long waitTime, TimeUnit timeUnit) throws InterruptedException;

  /**
   * 释放1个许可资源
   *
   * @throws SignalException 如果当前线程没有持有许可，则会抛出错误
   */
  default void release() {
    this.release(1);
  }

  /**
   * 释放1个或者多个许可资源
   *
   * @param permits 许可数量
   * @throws SignalException 如果当前线程没有持有许可，则会抛出错误
   */
  void release(int permits);

  /**
   * @return 返回初始化的许可数量
   */
  int permits();
}
