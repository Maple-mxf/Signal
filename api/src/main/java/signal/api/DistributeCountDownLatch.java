package signal.api;

public interface DistributeCountDownLatch extends DistributeSignal, Shared {

  int count();

  void countDown();

  void await() throws InterruptedException;
}
