package signal.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import org.bson.Document;
import org.bson.conversions.Bson;
import signal.api.Holder;

final class Utils {

  static Document mappedHolder2Bson(String lease, Holder holder) {
    return new Document("hostname", holder.hostname())
        .append("thread", holder.thread())
        .append("lease", lease);
  }

  static Holder mappedDoc2Holder(Document document) {
    return Holder.newHolder(
        document.getString("hostname"), document.getLong("thread"), document.getString("lease"));
  }

  static Bson mappedHolder2AndFilter(Document holder) {
    return and(
        eq("hostname", holder.get("hostname")),
        eq("thread", holder.get("thread")),
        eq("lease", holder.get("lease")));
  }

}
