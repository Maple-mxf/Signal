package signal.api;

import com.google.auto.value.AutoValue;

import java.util.UUID;

@AutoValue
public abstract class Holder {
  public abstract String hostname();

  public abstract long thread();

  public abstract String leaseId();

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
