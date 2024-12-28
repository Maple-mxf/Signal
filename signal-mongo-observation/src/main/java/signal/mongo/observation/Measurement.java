package signal.mongo.observation;

public class Measurement {
  public String leaseId;
  public String signalKey;
  public long startNanos;
  public long endNanos;
  public boolean thrownError;
  public String errorDesc;
  public int execCmdCount;
}
