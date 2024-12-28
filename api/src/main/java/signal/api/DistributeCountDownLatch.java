package signal.api;

import com.google.errorprone.annotations.ThreadSafe;

import java.util.concurrent.TimeUnit;

/** 分布式计数器实现 */
@ThreadSafe
public interface DistributeCountDownLatch extends DistributeSignal, Shared {

  /**
   * @return 返回需要计数的基数
   */
  int count();

  /** 执行向下计数 减1操作 */
  void countDown();

  /**
   * 挂起当前线程无限等待，当{@link DistributeCountDownLatch#count()}等于0时唤醒当前线程
   *
   * @throws InterruptedException 主线程deadline，wait状态的线程会抛出此错误
   */
  void await() throws InterruptedException;

  /**
   * 挂起当前线程等待，当{@link DistributeCountDownLatch#count()}等于0时唤醒当前线程
   *
   * @param waitTime 等待时长
   * @param timeUnit 事件单位
   * @throws InterruptedException 主线程deadline，wait状态的线程会抛出此错误
   * @throws SignalException 当等待超时抛出Timeout错误
   */
  void await(long waitTime, TimeUnit timeUnit) throws InterruptedException;
}
