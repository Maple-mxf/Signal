package signal.mongo;

import java.util.Objects;

final class StateVars<T> {
  T value;
  long revision;

  StateVars(T value, long revision) {
    this.value = value;
    this.revision = revision;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StateVars<?> stateVars = (StateVars<?>) o;
    return revision == stateVars.revision && Objects.equals(value, stateVars.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, revision);
  }
}
