package signal.mongo;

import org.junit.Test;

import java.util.Collection;

public class MongoSignalClientTest extends BaseResourceSetup {

    @Test
    public void testFindInvalidLeaseFromExclusiveSignal() {
        Collection<MongoSignalClient.ExpiredSignalCollNamedBind> invalidLeaseFromExclusiveSignals =
                signalClient.findExpireExclusiveSignal(
                        CollectionNamed.MUTEX_LOCK_NAMED
                );
        System.out.println(invalidLeaseFromExclusiveSignals.size());
    }

}
