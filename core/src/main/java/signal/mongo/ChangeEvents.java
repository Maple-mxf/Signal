package signal.mongo;

import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.Document;

interface ChangeEvents {
  record BarrierChangeEvent(String key) implements ChangeEvents {}

  record DoubleBarrierChangeEvent(
      String key,
      // 当对应删除操作时，fullDocument为空
      @Nullable Document fullDocument)
      implements ChangeEvents {}

  record CountDownLatchChangeEvent(
      String key,
      int cc,
      // 当对应删除操作时，fullDocument为空
      Document fullDocument)
      implements ChangeEvents {}

  record SemaphoreChangeEvent(
      @NonNull String key,
      // 当对应删除操作时，fullDocument为空
      Document fullDocument) {}

  record RWLockChangeEvent(
      @NonNull String key,
      // 当对应删除操作时，fullDocument为空
      Document fullDocument) {}

  record MutexLockChangeEvent(
      @NonNull String key,
      // 当对应删除操作时，fullDocument为空
      @Nullable Document fullDocument) {}
}
