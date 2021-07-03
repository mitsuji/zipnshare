package azure;

public class AzureBlobCleanerV12 implements Runnable {

    private AzureBlobBackgroundJobV12 backgroundJob;
    public AzureBlobCleanerV12 (String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String blobServiceContainer) {
	backgroundJob = new AzureBlobBackgroundJobV12(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase, storageAccountCS, blobServiceContainer);
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

