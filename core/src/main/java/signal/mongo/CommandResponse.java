package signal.mongo;

import com.mongodb.MongoException;

record CommandResponse<T>(
    T body,
    boolean occurDBError,
    boolean occurUnexpectedError,
    boolean dbErrorRetryable,
    boolean unexpectedErrorRetryable,
    MongoErrorCode dbErrorCode,
    Throwable cause) {
  static <T> CommandResponse<T> ok(T body) {
    return new CommandResponse<>(body, false, false, false, false, null, null);
  }

  static <T> CommandResponse<T> dbError(
      boolean dbErrorRetryable, MongoErrorCode dbErrorCode, MongoException cause) {
    return new CommandResponse<>(null, true, false, dbErrorRetryable, false, dbErrorCode, cause);
  }

  static <T> CommandResponse<T> unexpectedError(Throwable cause, boolean unexpectedErrorRetryable) {
    return new CommandResponse<>(null, false, true, false, unexpectedErrorRetryable, null, cause);
  }
}
