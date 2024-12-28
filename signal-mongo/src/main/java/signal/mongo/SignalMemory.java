package signal.mongo;

import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import signal.api.DistributeMutexLock;
import signal.api.DistributeSignal;

import java.util.HashMap;

abstract class SignalMemory<SignalType extends DistributeSignal> {
  final HashMap<String, SignalType> signalList = new HashMap<>(8, 0.8F);

  @Nullable
  SignalType get(String key) {
    return signalList.get(key);
  }

  void add(@NonNull SignalType signal) {
    signalList.put(signal.getKey(), signal);
  }

  static class DistributeMutexMemory extends SignalMemory<DistributeMutexLock> {}
}
