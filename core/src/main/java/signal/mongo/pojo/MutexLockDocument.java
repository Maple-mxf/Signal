package signal.mongo.pojo;

import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

public record MutexLockDocument(
    @BsonId String key,
    @BsonProperty("owner") MutexLockOwnerDocument owner,
    @BsonProperty("version") Long version) {}
