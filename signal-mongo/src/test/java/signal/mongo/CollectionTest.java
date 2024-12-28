package signal.mongo;

import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.Test;

public class CollectionTest extends BaseResourceSetup {

    @Test
    public void testGetMongoCollectionDefine() {
        MongoDatabase db = mongoClient.getDatabase(dbNamed);
        MongoCollection<Document> collection =
                db.getCollection(CollectionNamed.LEASE_NAMED);

        ListCollectionsIterable<Document> collections = db.listCollections();
        for (Document document : collections) {
            System.err.println(document.toJson());
        }
    }

    @Test
    public void testGetMongoCollectionIndex() {
        MongoDatabase db = mongoClient.getDatabase(dbNamed);
        MongoCollection<Document> collection = db.getCollection(CollectionNamed.LEASE_NAMED);
        for (Document index : collection.listIndexes()) {
            System.err.println(index.toJson());
        }
    }

}
