package signal.mongo.pojo;

import java.util.List;
import java.util.Optional;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

public record SemaphoreDocument(
    @BsonId String key,
    @BsonProperty("permits") Integer permits,
    @BsonProperty("owners") List<SemaphoreOwnerDocument> owners,
    @BsonProperty("version") Long version) {

  public boolean containOwner(SemaphoreOwnerDocument owner) {
    return this.owners.contains(owner);
  }

  public Optional<SemaphoreOwnerDocument> getThatOwner(SemaphoreOwnerDocument owner) {
    return this.owners.stream().filter(t -> t.equals(owner)).findFirst();
  }

  public int ownerCount() {
    return owners == null ? 0 : owners.size();
  }
}
