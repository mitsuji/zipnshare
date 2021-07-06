package azure;

public class AzureBlobCleanerV12 implements Runnable {

    private long cleanIntervalSeconds;
    private AzureBlobBackgroundJobV12 backgroundJob;
    public AzureBlobCleanerV12 (long cleanIntervalSeconds, long cleanExpiredSeconds, long cleanGarbageSeconds,
				String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String blobServiceContainer) {
	this.cleanIntervalSeconds = cleanIntervalSeconds;
	backgroundJob = new AzureBlobBackgroundJobV12(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase, storageAccountCS, blobServiceContainer);
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

