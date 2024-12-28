package signal.api;

import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.ThreadSafe;

/** DistributeSignalBase */
@ThreadSafe
public abstract class DistributeSignalBase implements DistributeSignal {
  /** lease */
  protected final Lease lease;

  /** key */
  protected final String key;

  /** closed */
  protected volatile boolean closed;

  /**
   * @param lease lease
   * @param key key
   */
  public DistributeSignalBase(Lease lease, String key) {
    this.lease = lease;
    this.key = key;
  }

  @CheckReturnValue
  @Override
  public Lease getLease() {
    return lease;
  }

  /**
   * @return 唯一Key
   */
  @Override
  public String getKey() {
    return key;
  }

  /** close */
  public synchronized void close() {
    if (this.closed) return;
    this.doClose();
    this.closed = true;
  }

  /** 执行关闭逻辑 */
  protected abstract void doClose();
}
