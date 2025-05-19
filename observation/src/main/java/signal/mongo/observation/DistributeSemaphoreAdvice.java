package signal.mongo.observation;

import net.bytebuddy.asm.Advice;
import signal.api.Lease;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class DistributeSemaphoreAdvice {

  static final ThreadLocal<Measurement> localVar = new ThreadLocal<>();

  @Advice.OnMethodEnter
  public static void onEnter(
      @Advice.This Object target,
      @Advice.Origin Method method,
      @Advice.AllArguments Object[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method getLease = target.getClass().getMethod("getLease");
    Lease lease = (Lease) getLease.invoke(target);
  }

  @Advice.OnMethodExit
  public static void enExit(
          @Advice.This Object target,
          @Advice.Origin Method method,
          @Advice.Thrown Throwable error) {}
}
