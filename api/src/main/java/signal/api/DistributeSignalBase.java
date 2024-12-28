package signal.api;

public abstract class DistributeSignalBase implements DistributeSignal {
    protected final Lease lease;
    protected final String key;
    protected volatile boolean closed;

    public DistributeSignalBase(
            Lease lease,
            String key) {
        this.lease = lease;
        this.key = key;
    }

    @Override
    public Lease getLease() {
        return lease;
    }

    @Override
    public String getKey() {
        return key;
    }

    public synchronized void close() {
        if (this.closed) return;
        this.doClose();
        this.closed = true;
    }

    protected abstract void doClose();
}
