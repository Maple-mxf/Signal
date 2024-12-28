package signal.api;

public interface DistributeSignal {

  Lease getLease();

  String getKey();

  void close();
}
