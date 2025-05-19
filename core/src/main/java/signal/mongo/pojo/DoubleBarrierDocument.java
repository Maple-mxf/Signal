package signal.mongo.pojo;

import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.List;
import java.util.Optional;

public record DoubleBarrierDocument(
    @BsonId String key,
    @BsonProperty("partner_num") int partnerNum,
    @BsonProperty("partners") List<DoubleBarrierPartnerDocument> partners,
    @BsonProperty("phase") DoubleBarrierPhase phase,
    @BsonProperty("version") Long version) {

  public Optional<DoubleBarrierPartnerDocument> getThatPartner(
      DoubleBarrierPartnerDocument partner) {
    return partners.stream().filter(t -> t.equals(partner)).findFirst();
  }
}
