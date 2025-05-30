package signal.mongo;

import com.google.errorprone.annotations.Immutable;

import java.util.Objects;

/**
 * TODO 需要评估是否需要增加数据的版本
 *
 * @param <T> 可以是基础类型，将普通变量保障成StateVar的目的是为了防止CAS更新的ABA问题 因为包装的对象可以会有1个唯一的内存地址
 */
@Immutable(containerOf = "T")
final class StatefulVar<T> {
  final T value;

  StatefulVar(T value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StatefulVar<?> stateVars = (StatefulVar<?>) o;
    return Objects.equals(value, stateVars.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public String toString() {
    return "StateVars{" + "value=" + value + '}';
  }
}
