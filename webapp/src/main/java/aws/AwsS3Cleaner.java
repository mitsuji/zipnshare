package aws;

public class AwsS3Cleaner implements Runnable {

    private long cleanIntervalSeconds;
    private AwsS3BackgroundJob backgroundJob;
    public AwsS3Cleaner (long cleanIntervalSeconds, long cleanExpiredSeconds, long cleanGarbageSeconds,
			 String region, String accessKeyId, String secretAccessKey, String dynamoTable, String s3Bucket) {
	this.cleanIntervalSeconds = cleanIntervalSeconds;
	backgroundJob = new AwsS3BackgroundJob(region, accessKeyId, secretAccessKey, dynamoTable, s3Bucket);
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

