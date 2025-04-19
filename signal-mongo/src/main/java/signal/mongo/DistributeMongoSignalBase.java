package signal.mongo;

import static com.mongodb.client.model.Filters.eq;
import static java.util.Collections.emptyList;
import static signal.mongo.MongoErrorCode.NoSuchTransaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.Keep;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import signal.api.DistributeSignalBase;
import signal.api.Holder;
import signal.api.Lease;

abstract class DistributeMongoSignalBase extends DistributeSignalBase {

    @Keep
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributeMongoSignalBase.class);

    final MongoClient mongoClient;
    final MongoCollection<Document> collection;

    static final TransactionOptions TRANSACTION_OPTIONS =
            TransactionOptions.builder()
                    .readPreference(ReadPreference.primary())
                    .readConcern(ReadConcern.MAJORITY)
                    .writeConcern(WriteConcern.MAJORITY)
                    .build();

    Document currHolder() {
        return Utils.mappedHolder2Bson(
                this.getLease().getLeaseID(),
                Holder.self(this.getLease().getLeaseID()));
    }

    static final FindOneAndUpdateOptions FU_UPSERT_OPTIONS =
            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER);

    static final FindOneAndUpdateOptions FU_UPDATE_OPTIONS =
            new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER);
    
    static final UpdateOptions UPDATE_OPTIONS = new UpdateOptions().upsert(false);

    /**
     * Command Executor
     */
    protected final CommandExecutor commandExecutor;

    public DistributeMongoSignalBase(
            Lease lease,
            String key,
            MongoClient mongoClient,
            MongoDatabase db,
            String collectionName) {
        super(lease, key);
        this.closed = false;
        this.mongoClient = mongoClient;
        this.collection = db.getCollection(collectionName);
        this.commandExecutor = new CommandExecutor(this, mongoClient, collection);
    }

    protected void checkState() {
        Preconditions.checkArgument(!getLease().isRevoked(), "Lease revoked.");
        Preconditions.checkState(!closed, "Semaphore instance closed.");
    }

    Optional<Document> extractHolder(Document signal, Document holder) {
        List<Document> holders = signal.getList("o", Document.class);
        if (holders == null || holders.isEmpty()) return Optional.empty();
        return holders.stream()
                .filter(
                        t ->
                                t.get("lease").equals(holder.get("lease"))
                                        && t.get("thread").equals(holder.get("thread"))
                                        && t.get("hostname").equals(holder.get("hostname")))
                .findFirst();
    }

    protected Collection<Holder> doGetHolders() {
        BiFunction<ClientSession, MongoCollection<Document>, Collection<Holder>> command =
                (session, coll) -> {
                    Document signal = coll.find(session, eq("_id", this.getKey())).first();
                    if (signal == null) return emptyList();

                    List<Document> holders = signal.getList("o", Document.class);
                    if (holders == null || holders.isEmpty()) return emptyList();
                    return Lists.transform(holders, Utils::mappedDoc2Holder);
                };
        return commandExecutor.loopExecute(
                command, commandExecutor.defaultDBErrorHandlePolicy(NoSuchTransaction), null, t -> false);
    }

    protected Holder doGetFirstHolder() {
        return doGetHolders().stream().findFirst().orElse(null);
    }

    protected boolean doIsHeldCurrentThread() {
        Document holder = currHolder();
        return doGetHolders().stream()
                .anyMatch(
                        t ->
                                t.thread() == holder.getLong("thread")
                                        && t.hostname().equals(holder.getString("hostname"))
                                        && t.leaseId().equals(holder.getString("lease")));
    }
}
