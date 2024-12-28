package signal.api;

public interface DistributeDoubleBarrier extends DistributeSignal, Shared {

  int participants();

  void enter() throws InterruptedException;

  void leave() throws InterruptedException;
}
