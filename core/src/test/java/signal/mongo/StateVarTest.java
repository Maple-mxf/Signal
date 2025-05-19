package signal.mongo;

import org.junit.Before;
import org.junit.Test;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StateVarTest {

  VarHandle varHandle;

  StatefulVar<Long> stateVars;

  @Before
  public void setup() throws NoSuchFieldException, IllegalAccessException {
    stateVars = new StatefulVar<>(0L);
    varHandle =
        MethodHandles.lookup().findVarHandle(StateVarTest.class, "stateVars", StatefulVar.class);
  }

  @Test
  public void test0() {
    StatefulVar<Long> _state = (StatefulVar<Long>) varHandle.getAcquire(this);
    int c = System.identityHashCode(_state);
    Object object =
        varHandle.compareAndExchangeRelease(this, _state, new StatefulVar<>(_state.value + 1L));
    System.err.println(c);
    System.err.println(System.identityHashCode(object));
    System.err.println(stateVars.value);
  }

  @Test
  public void testStateVarCasUpdate() throws InterruptedException {
    var countDown = new CountDownLatch(30);
    Runnable casUpdateTask =
        () -> {
          countDown.countDown();
          try {
            countDown.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

          for (; ; ) {
            StatefulVar<Long> _state = (StatefulVar<Long>) varHandle.getAcquire(this);
            int c = System.identityHashCode(_state);
            Object object =
                varHandle.compareAndExchangeRelease(
                    this, _state, new StatefulVar<>(_state.value + 1L));
            if (c == System.identityHashCode(object)) break;
            System.err.println(11111);
          }
        };

    ExecutorService executorService = Executors.newFixedThreadPool(30);
    for (int i = 0; i < 30; i++) {
      executorService.submit(casUpdateTask);
    }

    TimeUnit.SECONDS.sleep(3L);
    System.err.println(stateVars.value);
  }
}
