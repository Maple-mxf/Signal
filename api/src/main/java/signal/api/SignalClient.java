package signal.api;

import java.io.Closeable;

/** 分布式信号量客户端工具 */
public interface SignalClient extends Closeable {

  /**
   * 申请一个租约
   *
   * @param config 租约配置
   * @return 租约上下文
   */
  Lease grantLease(LeaseCreateConfig config);
}
