package signal.api;

import java.time.Instant;

/** Lease */
public abstract class Lease {
  /**
   * 全局唯一ID
   */
  protected final String leaseID;
  /**
   * 创建者
   */
  protected final Holder holder;

  /**
   * 创建时间
   */
  protected final Instant createdTime;

  /**
   * @param leaseID 全局唯一ID
   * @param holder 创建者
   * @param createdTime 创建时间
   */
  public Lease(String leaseID, Holder holder, Instant createdTime) {
    this.leaseID = leaseID;
    this.holder = holder;
    this.createdTime = createdTime;
  }

  /**
   * @return leaseID
   */
  public String getLeaseID() {
    return leaseID;
  }

  /**
   * @return holder
   */
  public Holder getHolder() {
    return holder;
  }

  /**
   * @return createdTime
   */
  public Instant getCreatedTime() {
    return createdTime;
  }

  /** 销毁当前租约 */
  public abstract void revoke();

  /**
   * @return 是否被销毁
   */
  public abstract boolean isRevoked();

  /**
   * @param key 唯一Key
   * @param count 计数
   * @return DistributeCountDownLatch
   */
  public abstract DistributeCountDownLatch getCountDownLatch(String key, int count);

  /**
   * @param key 唯一Key
   * @return DistributeMutexLock
   */
  public abstract DistributeMutexLock getMutexLock(String key);

  /**
   * @param key 唯一Key
   * @return DistributeReadWriteLock
   */
  public abstract DistributeReadWriteLock getReadWriteLock(String key);

  /**
   * @param key 唯一Key
   * @param permits 许可数量
   * @return DistributeSemaphore
   */
  public abstract DistributeSemaphore getSemaphore(String key, int permits);

  /**
   * @param key 唯一Key
   * @return DistributeBarrier
   */
  public abstract DistributeBarrier getBarrier(String key);

  /**
   * @param key 唯一Key
   * @param participants 参与者数量
   * @return getDoubleBarrier
   */
  public abstract DistributeDoubleBarrier getDoubleBarrier(String key, int participants);
}
