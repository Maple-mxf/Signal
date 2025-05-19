package signal.mongo.pojo;

import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.List;

public record CountDownLatchDocument(
    @BsonId String key,
    @BsonProperty("count") Integer count,
    @BsonProperty("cc") Integer cc,
    @BsonProperty("waiters") List<CountDownLatchWaiterDocument> waiters,
    @BsonProperty("version") Long version) {

  public boolean containWaiter(CountDownLatchWaiterDocument waiter) {
    return this.waiters.contains(waiter);
  }
}
