package signal.api;

/** 可重入接口 */
public interface Reentrancy {

  /**
   * @return 返回获取持有锁资源的线程重入次数
   */
  long getEnterCount();
}
