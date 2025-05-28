package signal.mongo;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.Set;
import org.junit.Test;
import signal.mongo.pojo.RWLockMode;

public class VarHandleTest {

  public VarHandleTest() throws NoSuchFieldException, IllegalAccessException {
    this.state = new StatefulVar<>(null);
    this.varHandle =
        MethodHandles.lookup().findVarHandle(VarHandleTest.class, "state", StatefulVar.class);
  }

  private record LockStateObject(RWLockMode mode, Set<Integer> ownerHashCodes) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LockStateObject that = (LockStateObject) o;
      return mode == that.mode && Objects.equals(ownerHashCodes, that.ownerHashCodes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mode, ownerHashCodes);
    }
  }

  private final VarHandle varHandle;
  private StatefulVar<LockStateObject> state;

  private void safeUpdate() {
    Object s1 = varHandle.getAcquire(this);
    Object o =
        varHandle.compareAndExchangeRelease(
            this, s1, new StatefulVar<>(new LockStateObject(RWLockMode.WRITE, Set.of(11))));
    System.err.println(o);
  }

  private class InnerClass {
    public void run() {
      safeUpdate();
    }
  }

  @Test
  public void testCompareAndRelease() {
    new InnerClass().run();
  }
}
