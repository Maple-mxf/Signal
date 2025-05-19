package signal.mongo.pojo;

import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;
import java.util.List;

public record MutexLockDocument(
    @BsonId String key,
    @BsonProperty("owners") List<MutexLockOwnerDocument> owners,
    @BsonProperty("version") Long version) {}
