package azure;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class AzureBlobCleanerV8 implements Runnable {

    private AzureBlobBackgroundJobV8 backgroundJob;
    public AzureBlobCleanerV8 (String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String cloudBlobContainer) throws Exception{
	try {
	    backgroundJob = new AzureBlobBackgroundJobV8(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase, storageAccountCS, cloudBlobContainer);
	} catch (URISyntaxException | InvalidKeyException ex) {
	    throw new Exception("failed to create backgroundJob", ex);
	}
    }

    public void run () {
	while (true) {
	    try {
		backgroundJob.clean();
		Thread.sleep (60 * 1000);
	    } catch (InterruptedException ex) {
		break;
	    } catch (Exception ex) {
		// [TODO] log
	    }
	}
    }

}

