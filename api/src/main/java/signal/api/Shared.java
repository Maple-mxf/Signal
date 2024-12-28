package signal.api;

import java.util.Collection;

/** 分布式共享信号量接口 */
public interface Shared {

  /**
   * @return 返回持有共享信号量所有权的持有者信息 {@link Holder}
   */
  Collection<Holder> getHolders();
}
