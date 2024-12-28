package signal.mongo;

final class CollectionNamed {
  private CollectionNamed() {}

  static final String LEASE_NAMED = "signal_lease";
  static final String BARRIER_NAMED = "signal_barrier";
  static final String DOUBLE_BARRIER_NAMED = "signal_double_barrier";
  static final String COUNT_DOWN_LATCH_NAMED = "signal_count_down_latch";
  static final String MUTEX_LOCK_NAMED = "signal_mutex_lock";
  static final String READ_WRITE_LOCK_NAMED = "signal_rw_lock";
  static final String SEMAPHORE_NAMED = "signal_semaphore";
}
