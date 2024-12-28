package signal.api;

import java.time.Instant;

public abstract class Lease {
    protected final String leaseID;
    protected final Holder holder;
    protected final Instant createdTime;

    public Lease(String leaseID,
                 Holder holder,
                 Instant createdTime) {
        this.leaseID = leaseID;
        this.holder = holder;
        this.createdTime = createdTime;
    }

    public String getLeaseID() {
        return leaseID;
    }

    public Holder getHolder() {
        return holder;
    }

    public Instant getCreatedTime() {
        return createdTime;
    }


    public abstract void revoke();

    public abstract boolean isRevoked();

    public abstract DistributeCountDownLatch getCountDownLatch(String key, int count);

    public abstract DistributeMutexLock getMutexLock(String key);

    public abstract DistributeReadWriteLock getReadWriteLock(String key);

    public abstract DistributeSemaphore getSemaphore(String key, int permits);

    public abstract DistributeBarrier getBarrier(String key);

    public abstract DistributeDoubleBarrier getDoubleBarrier(String key, int participants);
}
