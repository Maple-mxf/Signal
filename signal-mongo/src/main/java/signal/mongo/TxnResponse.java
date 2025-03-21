package signal.mongo;

final class TxnResponse {
  public final boolean txnOk;
  public final boolean retryable;
  public final boolean thrownError;
  public final String message;
  public final boolean parkThread;

  TxnResponse(
      boolean txnOk, boolean retryable, boolean parkThread, boolean thrownError, String message) {
    this.txnOk = txnOk;
    this.retryable = retryable;
    this.thrownError = thrownError;
    this.message = message;
    this.parkThread = parkThread;
  }

  public static final TxnResponse OK = new TxnResponse(true, false, false, false, "");
  public static final TxnResponse RETRYABLE_ERROR = new TxnResponse(false, true, false, false, "");
  public static final TxnResponse PARK_THREAD = new TxnResponse(false, true, true, false, "");

  public static TxnResponse ok() {
    return OK;
  }

  public static TxnResponse thrownAnError(String message) {
    return new TxnResponse(false, false, false, true, message);
  }

  public static TxnResponse retryableError() {
    return RETRYABLE_ERROR;
  }

  public static TxnResponse parkThread() {
    return PARK_THREAD;
  }

  public static TxnResponse parkThreadWithSuccess() {
    return new TxnResponse(true, true, true, false, "");
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
