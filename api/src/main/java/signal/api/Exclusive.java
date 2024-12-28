package signal.api;

/** 独占信号量标记 */
public interface Exclusive {

  /**
   * @return 返回信号量持有者的信息
   */
  Holder getHolder();
}
