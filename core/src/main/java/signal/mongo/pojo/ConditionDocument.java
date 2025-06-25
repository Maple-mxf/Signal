package signal.mongo.pojo;

import java.util.List;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

public record ConditionDocument(
    @BsonId String conditionKey,
    @BsonProperty("lock_key") String lockKey,
    @BsonProperty("waiters") List<ConditionWaiterDocument> waiters,
    @BsonProperty("version") Long version,
    @BsonProperty("timestamp") Long timestamp) {

}
