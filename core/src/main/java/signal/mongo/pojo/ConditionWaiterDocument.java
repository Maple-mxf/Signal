package signal.mongo.pojo;

import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.Objects;

public record ConditionWaiterDocument(
    @BsonProperty("hostname") String hostname,
    @BsonProperty("lease") String lease,
    @BsonProperty("thread") String thread,
    @BsonProperty("status") Integer status) {

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConditionWaiterDocument that = (ConditionWaiterDocument) o;
    return Objects.equals(hostname, that.hostname)
        && Objects.equals(lease, that.lease)
        && Objects.equals(thread, that.thread);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostname, lease, thread);
  }
}
