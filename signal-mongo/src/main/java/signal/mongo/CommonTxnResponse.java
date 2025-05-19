package signal.mongo;

final class CommonTxnResponse {
  public final boolean txnOk;
  public final boolean retryable;
  public final boolean thrownError;
  public final String message;

  // TODO 此字段的作用待商榷  是否阻塞线程应该由各个类的状态变量决定
  @Deprecated public final boolean parkThread;

  CommonTxnResponse(
      boolean txnOk, boolean retryable, boolean parkThread, boolean thrownError, String message) {
    this.txnOk = txnOk;
    this.retryable = retryable;
    this.thrownError = thrownError;
    this.message = message;
    this.parkThread = parkThread;
  }

  public static final CommonTxnResponse OK = new CommonTxnResponse(true, false, false, false, "");
  public static final CommonTxnResponse RETRYABLE_ERROR =
      new CommonTxnResponse(false, true, false, false, "");

  @Deprecated
  public static final CommonTxnResponse PARK_THREAD =
      new CommonTxnResponse(false, true, true, false, "");

  public static CommonTxnResponse ok() {
    return OK;
  }

  public static CommonTxnResponse thrownAnError(String message) {
    return new CommonTxnResponse(false, false, false, true, message);
  }

  public static CommonTxnResponse retryableError() {
    return RETRYABLE_ERROR;
  }

  @Deprecated
  public static CommonTxnResponse parkThread() {
    return PARK_THREAD;
  }
  @Deprecated
  public static CommonTxnResponse parkThreadWithSuccess() {
    return new CommonTxnResponse(true, true, true, false, "");
  }

  @Override
  public String toString() {
    return "TxnResponse{"
        + "txnOk="
        + txnOk
        + ", retryable="
        + retryable
        + ", thrownError="
        + thrownError
        + ", message='"
        + message
        + '\''
        + ", parkThread="
        + parkThread
        + '}';
  }
}
