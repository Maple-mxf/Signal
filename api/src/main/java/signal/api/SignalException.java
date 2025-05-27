package signal.api;

/** 信号量异常 */
public class SignalException extends RuntimeException {

  /** default */
  public SignalException() {}

  /**
   * @param message 错误信息
   */
  public SignalException(String message) {
    super(message);
  }
}
