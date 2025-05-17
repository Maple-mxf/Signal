package signal.mongo.pojo;

import org.bson.codecs.pojo.annotations.BsonProperty;

public record BarrierWaiterDocument(
    @BsonProperty("hostname") String hostname,
    @BsonProperty("lease") String lease,
    @BsonProperty("thread") String thread) {}
