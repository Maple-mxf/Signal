package signal.api;

import java.io.Closeable;

public interface SignalClient extends Closeable {

    Lease grantLease(LeaseCreateConfig config);
}
