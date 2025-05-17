package signal.mongo.pojo;

import java.util.List;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

public record BarrierDocument(
    @BsonId String key,
    @BsonProperty("waiters") List<BarrierWaiterDocument> waiters,
    @BsonProperty("version") Long version) {}
