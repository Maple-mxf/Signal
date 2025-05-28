package signal.mongo;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import signal.api.DistributeSignalBase;
import signal.api.Lease;

abstract class DistributeMongoSignalBase<Doc> extends DistributeSignalBase {

  final MongoClient mongoClient;

  final TypeToken<Doc> documentTypeToken;

  final MongoCollection<Doc> collection;

  static final TransactionOptions TRANSACTION_OPTIONS =
      TransactionOptions.builder()
          .readPreference(ReadPreference.primary())
          .readConcern(ReadConcern.MAJORITY)
          .writeConcern(WriteConcern.MAJORITY)
          .build();

  static final FindOneAndUpdateOptions UPSERT_OPTIONS =
      new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER);

  static final FindOneAndUpdateOptions UPDATE_OPTIONS =
      new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER);

  /** Command Executor */
  protected final CommandExecutor<Doc> commandExecutor;

  @SuppressWarnings("unchecked")
  public DistributeMongoSignalBase(
      Lease lease, String key, MongoClient mongoClient, MongoDatabase db, String collectionName) {
    super(lease, key);
    this.closed = false;
    this.mongoClient = mongoClient;
    this.documentTypeToken = new TypeToken<>(getClass()) {};
    this.collection =
        (MongoCollection<Doc>) db.getCollection(collectionName, documentTypeToken.getRawType());
    this.commandExecutor = new CommandExecutor<Doc>(this, mongoClient, collection);
  }

  protected void checkState() {
    Preconditions.checkArgument(!getLease().isRevoked(), "Lease revoked.");
    Preconditions.checkState(!closed, "Semaphore instance closed.");
  }
}
