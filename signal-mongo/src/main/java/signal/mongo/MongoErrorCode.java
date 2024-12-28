package signal.mongo;

import com.mongodb.MongoException;
import com.mongodb.client.model.changestream.OperationType;

import java.util.Arrays;

/** <a href="https://www.mongodb.com/docs/manual/reference/error-codes/">MongoDB Error Code </a> */
enum MongoErrorCode {
  UnknownError(8, "UnknownError", false, false, false),
  InternalError(1, "InternalError", false, false, false),
  BadValue(2, "BadValue", false, false, false),
  HostUnreachable(6, "HostUnreachable", false, false, false),
  HostNotFound(7, "HostNotFound", false, false, false),
  Overflow(15, "Overflow", false, false, false),
  InvalidBSON(22, "InvalidBSON", false, false, false),
  LockTimeout(22, "LockTimeout", true, true, true),
  LockBusy(46, "LockBusy", true, true, true),
  NetworkTimeout(89, "NetworkTimeout", true, true, true),
  LockFailed(107, "LockFailed", true, true, true),
  WriteConflict(112, "WriteConflict", false, true, true),
  CommandNotSupported(115, "CommandNotSupported", false, false, false),
  NetworkInterfaceExceededTimeLimit(202, "NetworkInterfaceExceededTimeLimit", true, true, true),
  NoSuchTransaction(251, "NoSuchTransaction", true, true, true),
  ExceededTimeLimit(262, "ExceededTimeLimit", true, true, true),
  TooManyFilesOpen(264, "TooManyFilesOpen", true, true, true),
  TransactionExceededLifetimeLimitSeconds(
      290, "TransactionExceededLifetimeLimitSeconds", true, true, true),
  DuplicateKey(11000, "DuplicateKey", false, true, true),
  ;

  private final int code;
  private final String cause;

  @Deprecated private final boolean insertRetryable;
  @Deprecated private final boolean updateRetryable;
  @Deprecated private final boolean deleteRetryable;

  MongoErrorCode(
      int code,
      String cause,
      boolean insertRetryable,
      boolean updateRetryable,
      boolean deleteRetryable) {
    this.code = code;
    this.cause = cause;

    this.insertRetryable = insertRetryable;
    this.updateRetryable = updateRetryable;
    this.deleteRetryable = deleteRetryable;
  }

  public static MongoErrorCode fromException(MongoException error) {
    return Arrays.stream(MongoErrorCode.values())
        .filter(t -> t.code == error.getCode())
        .findFirst()
        .orElse(MongoErrorCode.UnknownError);
  }

  public int getCode() {
    return code;
  }

  public String getCause() {
    return cause;
  }

  public boolean isRetryable(OperationType operationType) {
    switch (operationType) {
      case DELETE -> {
        return this.deleteRetryable;
      }
      case INSERT -> {
        return this.insertRetryable;
      }
      case UPDATE -> {
        return this.updateRetryable;
      }
      default -> {
        return false;
      }
    }
  }
}
