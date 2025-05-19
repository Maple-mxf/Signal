package signal.mongo.pojo;

import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

public record RWLockOwnerDocument(
    @BsonId String key,
    @BsonProperty("mode") RWLockMode mode,
    @BsonProperty("version") Long version) {}
