package signal.api;

/**
 * 分布式互斥锁
 *
 * <p>特性
 *
 * <ul>
 *   <li>互斥性
 *   <li>可重入
 *   <li>阻塞性
 * </ul>
 */
public interface DistributeMutexLock extends DistributeLock, Exclusive, Reentrancy {}
