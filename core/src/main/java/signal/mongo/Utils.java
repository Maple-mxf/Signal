package signal.mongo;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import signal.api.SignalException;

final class Utils {

  /**
   * 此方法不再需要再次Lock，事务内部已经进行了Lock
   *
   * @param timed 是否开启超时限制
   * @param s 开始事件
   * @param waitTimeNanos 等待的时间
   * @return 如果等待超时 返回true 否则false
   * @throws InterruptedException 如果主线程被shutdown，当前等待的线程会触发此错误
   */
  static boolean parkCurrentThreadUntil(
      ReentrantLock lock,
      Condition condition,
      Supplier<Boolean> parkCond,
      boolean timed,
      long s,
      long waitTimeNanos)
      throws InterruptedException {
    lock.lock();
    try {
      while (parkCond.get()) {
        if (timed) {
          if (waitTimeNanos <= 0) throw new SignalException("Timeout.");
          return condition.await((waitTimeNanos - (nanoTime() - s)), NANOSECONDS);
        } else {
          condition.await();
          return true;
        }
      }
      return true;
    } finally {
      lock.unlock();
    }
  }

  static void unparkSuccessor(ReentrantLock lock, Condition condition, boolean all) {
    lock.lock();
    try {
      if (all) condition.signalAll();
      else condition.signal();
    } finally {
      lock.unlock();
    }
  }

  static boolean isBitSet(int num, int n) {
    return (num & (1 << n)) != 0;
  }

  static String getCurrentThreadName() {
    return String.format("%s-%d", Thread.currentThread().getName(), Thread.currentThread().getId());
  }

  static String getCurrentHostname() {
    return Optional.ofNullable(System.getenv("HOSTNAME")).orElse("");
  }

  public static void main(String[] args) {}
}
