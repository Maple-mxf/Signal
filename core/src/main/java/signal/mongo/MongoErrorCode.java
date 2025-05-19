package signal.mongo;

import com.mongodb.MongoException;
import java.util.Arrays;

/** <a href="https://www.mongodb.com/docs/manual/reference/error-codes/">MongoDB Error Code </a> */
enum MongoErrorCode {
  UnknownError(8, "UnknownError"),
  InternalError(1, "InternalError"),
  BadValue(2, "BadValue"),
  HostUnreachable(6, "HostUnreachable"),
  HostNotFound(7, "HostNotFound"),
  Overflow(15, "Overflow"),
  InvalidBSON(22, "InvalidBSON"),
  LockTimeout(22, "LockTimeout"),
  LockBusy(46, "LockBusy"),
  NetworkTimeout(89, "NetworkTimeout"),
  LockFailed(107, "LockFailed"),
  WriteConflict(112, "WriteConflict"),
  CommandNotSupported(115, "CommandNotSupported"),
  NetworkInterfaceExceededTimeLimit(202, "NetworkInterfaceExceededTimeLimit"),
  NoSuchTransaction(251, "NoSuchTransaction"),
  ExceededTimeLimit(262, "ExceededTimeLimit"),
  TooManyFilesOpen(264, "TooManyFilesOpen"),
  TransactionExceededLifetimeLimitSeconds(290, "TransactionExceededLifetimeLimitSeconds"),
  DuplicateKey(11000, "DuplicateKey"),
  ;

  private final int code;
  private final String cause;

  MongoErrorCode(int code, String cause) {
    this.code = code;
    this.cause = cause;
  }

  static MongoErrorCode fromException(MongoException error) {
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
}
