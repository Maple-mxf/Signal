package signal.api;

import com.google.errorprone.annotations.ThreadSafe;

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
@ThreadSafe
public interface DistributeMutexLock extends DistributeLock, Exclusive, Reentrancy {}
