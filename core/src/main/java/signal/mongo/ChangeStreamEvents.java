package signal.mongo;

import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.Document;

interface ChangeStreamEvents {
  record BarrierRemovedEvent(String barrierKey, Document beforeChangeDocument)
      implements ChangeStreamEvents {}

  record DoubleBarrierChangeEvent(
      String doubleBarrierKey,
      int participants,
      // 当对应删除操作时，fullDocument为空
      Document fullDocument)
      implements ChangeStreamEvents {}

  record CountDownLatchChangeEvent(
      String cdlKey,
      int c,
      int cc,
      // 当对应删除操作时，fullDocument为空
      Document fullDocument)
      implements ChangeStreamEvents {}

  record SemaphoreChangeAndRemovedEvent(
      @NonNull String semaphoreKey,
      int permits,
      // 当对应删除操作时，fullDocument为空
      Document fullDocument) {}

  record ReadWriteLockChangeAndRemovedEvent(
      @NonNull String lockKey,
      // 当对应删除操作时，fullDocument为空
      Document fullDocument) {}

  record MutexLockChangeAndRemoveEvent(
      @NonNull String lockKey,
      // 当对应删除操作时，fullDocument为空
      @Nullable Document fullDocument) {}
}
