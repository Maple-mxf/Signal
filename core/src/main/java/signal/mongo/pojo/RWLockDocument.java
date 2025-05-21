package signal.mongo.pojo;

import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.List;

public record RWLockDocument(
    @BsonId String key,
    @BsonProperty("mode") RWLockMode mode,
    @BsonProperty("owners") List<RWLockOwnerDocument> owners,
    @BsonProperty("version") Long version) {}
