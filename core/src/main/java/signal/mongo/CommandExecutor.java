package signal.mongo;

import static java.lang.System.nanoTime;
import static signal.mongo.DistributeMongoSignalBase.TRANSACTION_OPTIONS;

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CheckReturnValue;
import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import signal.api.SignalException;

/**
 * <a href="https://www.mongodb.com/docs/manual/core/transactions/">MongoDB Transaction Java Driver
 * API </a>
 *
 * <p><a href="https://www.mongodb.com/docs/drivers/java/sync/v5.2/compatibility/">MongoDB Java
 * Driver Version Compatibility</a>
 *
 * <p>事务必须要重试的错误
 *
 * <ul>
 *   <li>持久化错误 {@link MongoException#TRANSIENT_TRANSACTION_ERROR_LABEL}
 *   <li>未知错误 {@link MongoException#UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL}
 * </ul>
 *
 * <p>从MongoDB6.2开始, 当遇到{@link CommandExecutor#TRANSACTION_TOO_LARGE_ERROR_LABEL}错误时， 事务将不再重试 <a
 * href="https://www.mongodb.com/docs/manual/core/transactions-in-applications/#std-label-transactionTooLargeForCache-error">
 * TransactionTooLargeForCache</a>
 */
final class CommandExecutor<Doc> {
  private final MongoClient mongoClient;
  private final MongoCollection<Doc> collection;
  private final Object lockObj;

  public static final String TRANSACTION_TOO_LARGE_ERROR_LABEL = "TransactionTooLargeForCache";

  CommandExecutor(Object lockObj, MongoClient mongoClient, MongoCollection<Doc> collection) {
    this.mongoClient = mongoClient;
    this.collection = collection;
    this.lockObj = lockObj;
  }

  <T> BiFunction<MongoException, MongoErrorCode, CommandResponse<T>> defaultDBErrorHandlePolicy(
      @NonNull MongoErrorCode... retryableCodes) {
    return (dbError, dbErrorCode) ->
        CommandResponse.dbError(
            !dbError.hasErrorLabel(TRANSACTION_TOO_LARGE_ERROR_LABEL)
                && ImmutableSet.copyOf(retryableCodes).contains(dbErrorCode),
            dbErrorCode,
            dbError);
  }

  @CheckReturnValue
  <T> T loopExecute(
      @NonNull BiFunction<ClientSession, MongoCollection<Doc>, T> command,
      @Nullable
          BiFunction<MongoException, MongoErrorCode, CommandResponse<T>> dbErrorHandlePolicy) {
    return loopExecute(command, dbErrorHandlePolicy, null, t -> false);
  }

  @CheckReturnValue
  <T> T loopExecute(
      @NonNull BiFunction<ClientSession, MongoCollection<Doc>, T> command,
      @Nullable BiFunction<MongoException, MongoErrorCode, CommandResponse<T>> dbErrorHandlePolicy,
      @Nullable Function<Throwable, CommandResponse<T>> unexpectedErrorHandlePolicy,
      Predicate<T> resultHandlePolicy) {
    return loopExecute(
        command,
        dbErrorHandlePolicy,
        unexpectedErrorHandlePolicy,
        resultHandlePolicy,
        false,
        -1L,
        TimeUnit.NANOSECONDS);
  }

  @CheckReturnValue
  <T> T loopExecute(
      @NonNull BiFunction<ClientSession, MongoCollection<Doc>, T> command,
      @Nullable BiFunction<MongoException, MongoErrorCode, CommandResponse<T>> dbErrorHandlePolicy,
      @Nullable Function<Throwable, CommandResponse<T>> unexpectedErrorHandlePolicy,
      Predicate<T> resultHandlePolicy,
      boolean timed,
      long waitTime,
      TimeUnit timeUnit) {

    long waitTimeNanos = timeUnit.toNanos(waitTime), s = nanoTime();

    for (; ; ) {
      CommandResponse<T> response =
          execute(command, dbErrorHandlePolicy, unexpectedErrorHandlePolicy);
      if ((timed
              && (nanoTime() - s) >= waitTimeNanos
              && (response.occurDBError() || response.occurUnexpectedError()))
          || (timed && waitTimeNanos <= 0)) {
        throw new SignalException("Timeout.");
      }

      // 如果触发了MongoDB错误并且错误不可重试
      if (response.occurDBError() && !response.dbErrorRetryable()) {
        throw new SignalException(response.cause().getMessage());
      }

      // 重试DB错误
      if (response.occurDBError()) {
        Thread.onSpinWait();
        continue;
      }

      // 如果触发了意外错误并且错误不可重试
      if (response.occurUnexpectedError() && !response.unexpectedErrorRetryable()) {
        throw new SignalException(response.cause().getMessage());
      }

      // 重试意外错误
      if (response.occurUnexpectedError()) {
        Thread.onSpinWait();
        continue;
      }

      if (resultHandlePolicy.test(response.body())) {
        Thread.onSpinWait();
        continue;
      }
      return response.body();
    }
  }

  @CheckReturnValue
  <T> CommandResponse<T> execute(
      @NonNull BiFunction<ClientSession, MongoCollection<Doc>, T> command,
      @Nullable BiFunction<MongoException, MongoErrorCode, CommandResponse<T>> dbErrorHandlePolicy,
      @Nullable Function<Throwable, CommandResponse<T>> unexpectedErrorHandlePolicy) {

    // 在单个进程内部 顺序执行事务操作 避免CPU飙升
    synchronized (lockObj) {
      try (ClientSession session = mongoClient.startSession()) {
        System.err.println("execute LoopCommand");
        T commandBody =
            session.withTransaction(() -> command.apply(session, collection), TRANSACTION_OPTIONS);
        return CommandResponse.ok(commandBody);
      } catch (MongoException dbError) {
        MongoErrorCode errCode = MongoErrorCode.fromException(dbError);
        System.err.println("execute LoopCommand MongoErrorCode : "  + errCode);
        return dbErrorHandlePolicy != null
            ? dbErrorHandlePolicy.apply(dbError, errCode)
            : CommandResponse.dbError(false, errCode, dbError);
      } catch (Throwable unexpectedError) {
        System.err.println("execute LoopCommand unexpectedError : "  + unexpectedError.getMessage());
        return unexpectedErrorHandlePolicy != null
            ? unexpectedErrorHandlePolicy.apply(unexpectedError)
            : CommandResponse.unexpectedError(unexpectedError, false);
      }
    }
  }
}
