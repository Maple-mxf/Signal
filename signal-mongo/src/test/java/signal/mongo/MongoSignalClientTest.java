package signal.mongo;

import org.junit.Assert;
import org.junit.Test;

public class MongoSignalClientTest extends BaseResourceSetup {

    @Test
    public void testDispatcher() {
        Assert.assertNotNull(signalClient.getDispatcherHandler(
                CollectionNamed.LEASE_NAMED,
                0b001
        ));
        Assert.assertNotNull(signalClient.getDispatcherHandler(
                CollectionNamed.DOUBLE_BARRIER_NAMED,
                0b010
        ));
        Assert.assertNotNull(signalClient.getDispatcherHandler(
                CollectionNamed.COUNT_DOWN_LATCH_NAMED,
                0b010
        ));
        Assert.assertNotNull(signalClient.getDispatcherHandler(
                CollectionNamed.SEMAPHORE_NAMED,
                0b001
        ));
        Assert.assertNotNull(signalClient.getDispatcherHandler(
                CollectionNamed.SEMAPHORE_NAMED,
                0b010
        ));


      Assert.assertNotNull(signalClient.getDispatcherHandler(
              CollectionNamed.READ_WRITE_LOCK_NAMED,
              0b001
      ));
      Assert.assertNotNull(signalClient.getDispatcherHandler(
              CollectionNamed.READ_WRITE_LOCK_NAMED,
              0b010
      ));
      Assert.assertNotNull(signalClient.getDispatcherHandler(
              CollectionNamed.READ_WRITE_LOCK_NAMED,
              0b100
      ));

      Assert.assertNotNull(signalClient.getDispatcherHandler(
              CollectionNamed.MUTEX_LOCK_NAMED,
              0b001
      ));
      Assert.assertNotNull(signalClient.getDispatcherHandler(
              CollectionNamed.MUTEX_LOCK_NAMED,
              0b010
      ));

    }

}
