package signal.api;

// 读锁：允许多个线程同时持有读锁，只要没有线程持有写锁。这意味着在读取操作频繁且对一致性要求不高的场景下，多个线程可以同时读取共享资源
// 写锁: 一次只能有一个线程持有写锁，持有写锁时不允许其他线程获取读锁或写锁。这保证了在写入操作时，没有其他线程可以访问共享资源，从而确保了数据的一致性。
public interface DistributeReadWriteLock extends DistributeSignal {

  DistributeReadLock readLock();

  DistributeWriteLock writeLock();

  interface DistributeReadLock extends DistributeLock, Shared {}

  interface DistributeWriteLock extends DistributeLock, Exclusive {}
}
