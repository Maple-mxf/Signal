package signal.api;

import com.google.errorprone.annotations.ThreadSafe;

import java.io.Closeable;

/** 分布式信号量客户端工具 */
@ThreadSafe
public interface SignalClient extends Closeable {

  /**
   * 申请一个租约
   *
   * @param config 租约配置
   * @return 租约上下文
   */
  Lease grantLease(LeaseCreateConfig config);
}
