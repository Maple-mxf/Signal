package signal.api;

import com.google.auto.value.AutoValue;
import com.google.errorprone.annotations.CheckReturnValue;

import java.util.UUID;

/** 资源持有者的信息 */
@AutoValue
public abstract class Holder {

  /**
   * @return 返回当前主机信息
   */
  public abstract String hostname();

  /**
   * @return JVM Thread ID
   */
  public abstract long thread();

  /**
   * @return Lease的上下文ID
   */
  public abstract String leaseId();

  /**
   * @param hostname 主机
   * @param thread 线程
   * @param leaseId 租约上下文ID
   * @return 持有者对象
   */
  @CheckReturnValue
  public static Holder newHolder(String hostname, long thread, String leaseId) {
    return new AutoValue_Holder(hostname, thread, leaseId);
  }

  static final String HOSTNAME;

  static {
    String hostname = System.getenv("hostname");
    HOSTNAME =
        (hostname == null || hostname.isEmpty())
            ? generateFixedLengthRandomString(6).toUpperCase()
            : String.format("%s-%s", hostname, generateFixedLengthRandomString(4).toUpperCase());
  }

  /**
   * @param leaseId 上下文ID
   * @return 当前Holder信息
   */
  @CheckReturnValue
  public static Holder self(String leaseId) {
    return new AutoValue_Holder(HOSTNAME, Thread.currentThread().getId(), leaseId);
  }

  static String generateFixedLengthRandomString(int length) {
    String uuid = UUID.randomUUID().toString().replace("-", "");
    if (length > uuid.length()) {
      throw new IllegalArgumentException("Requested length is too long");
    }
    return uuid.substring(0, length);
  }
}
