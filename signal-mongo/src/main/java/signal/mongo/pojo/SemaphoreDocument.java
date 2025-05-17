package signal.mongo.pojo;

import java.util.List;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

public record SemaphoreDocument(
    @BsonId String key,
    @BsonProperty("permits") Integer permits,
    @BsonProperty("owners") List<SemaphoreOwnerDocument> owners,
    @BsonProperty("version") Long version) {}
