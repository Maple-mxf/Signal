package signal.api;

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
public interface DistributeBarrier extends DistributeSignal, Shared {

  void setBarrier();

  void waitOnBarrier() throws InterruptedException;

  void removeBarrier();

  int getHoldCount();
}
