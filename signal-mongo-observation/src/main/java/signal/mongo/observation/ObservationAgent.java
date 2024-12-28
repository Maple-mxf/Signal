package signal.mongo.observation;

import java.lang.instrument.Instrumentation;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;

public class ObservationAgent {

  public static void premain(String agentArgs, Instrumentation inst) {

    new AgentBuilder.Default()
        .type(ElementMatchers.nameContains("DistributeSemaphore"))
        .transform(
            (builder, type, classLoader, module, protectionDomain) ->
                builder
                    .method(ElementMatchers.named(""))
                    .intercept(MethodDelegation.to(DistributeSemaphoreAdvice.class)))
        .installOn(inst);
  }
}
