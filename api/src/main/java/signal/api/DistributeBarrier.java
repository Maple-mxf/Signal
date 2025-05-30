package signal.api;

import com.google.errorprone.annotations.ThreadSafe;

import java.util.concurrent.TimeUnit;

/**
 *
 *
 * <pre> Barrier Example
 *     {@code
 *     Lease lease = ...;
 *     Barrier barrier = lease.getBarrier("Test-Barrier");
 *     barrier.setBarrier();
 *     int concurrency = 12;
 *     for (int i = 0; i < concurrency; i++) {
 *       CompletableFuture.runAsync(
 *           () -> {
 *             try {
 *               LOGGER.debug(
 *                   "Prepare waiton the barrier ThreadId = {}", Thread.currentThread().getId());
 *               barrier.waitOnBarrier();
 *               LOGGER.debug("Leave the barrier ThreadId = {}", Thread.currentThread().getId());
 *             } catch (Throwable e) {
 *               e.printStackTrace();
 *             }
 *           },
 *           executorService);
 *     }
 *     while (concurrency != barrier.getHoldCount()) {
 *       Thread.onSpinWait();
 *     }
 *     barrier.removeBarrier();
 *     LOGGER.debug("Barrier has been removed.");
 *
 *     lease.revoke();
 *     }
 * </pre>
 */
@ThreadSafe
public interface DistributeBarrier extends DistributeSignal, Shared {

  /**
   * 等待障碍移除
   *
   * @throws InterruptedException 主线程deadline，wait状态的线程会抛出此错误
   */
  void await(Long waitTime, TimeUnit timeUnit) throws InterruptedException;

  default void await() throws InterruptedException {
    await(-1L, TimeUnit.MILLISECONDS);
  }

  /** 移除障碍 */
  void removeBarrier();

  /**
   * @return 返回当前到达障碍处的线程数量
   */
  int getWaiterCount();
}
