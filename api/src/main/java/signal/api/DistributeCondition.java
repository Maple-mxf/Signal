package signal.api;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public interface DistributeCondition extends Condition {

  String conditionKey();

  @Override
  void await() throws InterruptedException;

  @Override
  void awaitUninterruptibly();

  @Override
  long awaitNanos(long nanosTimeout) throws InterruptedException;

  @Override
  boolean await(long time, TimeUnit unit) throws InterruptedException;

  @Override
  boolean awaitUntil(Date deadline) throws InterruptedException;

  @Override
  void signal();

  @Override
  void signalAll();
}
