package signal.api;

import com.google.errorprone.annotations.ThreadSafe;

/** 分布式Double Barrier实现 当参与者进入enter状态时，执行线程挂起，当所有参与者都处于enter状态时， 执行线程唤醒 */
@ThreadSafe
public interface DistributeDoubleBarrier extends DistributeSignal, Shared {

  /**
   * @return 返回参与者的数量
   */
  int participants();

  /**
   * 参与者进入enter状态，线程挂起，当所有参与者都到达时，线程会被唤醒
   *
   * @throws InterruptedException 主线程deadline，wait状态的线程会抛出此错误
   */
  void enter() throws InterruptedException;

  /**
   * 参与者进入leave状态，线程挂起，当所有参与者都到达时，线程会被唤醒
   *
   * @throws InterruptedException 主线程deadline，wait状态的线程会抛出此错误
   */
  void leave() throws InterruptedException;
}
