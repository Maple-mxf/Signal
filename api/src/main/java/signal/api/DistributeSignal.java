package signal.api;

/** 分布式信号顶级接口 */
public interface DistributeSignal extends AutoCloseable {

  /**
   * @return 返回上下文租约
   */
  Lease getLease();

  /**
   * @return 返回当前信号量的唯一标识符
   */
  String getKey();

  /** 清楚当前信号量 */
  void close();
}
