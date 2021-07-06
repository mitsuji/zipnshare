package azure;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class AzureBlobCleanerV8 implements Runnable {

    private long cleanIntervalSeconds;
    private AzureBlobBackgroundJobV8 backgroundJob;
    public AzureBlobCleanerV8 (long cleanIntervalSeconds, long cleanExpiredSeconds, long cleanGarbageSeconds,
			       String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String cloudBlobContainer) throws Exception{
	this.cleanIntervalSeconds = cleanIntervalSeconds;
	try {
	    backgroundJob = new AzureBlobBackgroundJobV8(cleanExpiredSeconds, cleanGarbageSeconds,
							 cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase, storageAccountCS, cloudBlobContainer);
	} catch (URISyntaxException | InvalidKeyException ex) {
	    throw new Exception("failed to create backgroundJob", ex);
	}
    }

    public void run () {
	while (true) {
	    try {
		backgroundJob.clean();
		Thread.sleep (cleanIntervalSeconds * 1000);
	    } catch (InterruptedException ex) {
		break;
	    } catch (Exception ex) {
		// [TODO] log
		ex.printStackTrace();
	    }
	}
    }

}

