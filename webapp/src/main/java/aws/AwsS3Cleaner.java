package aws;

public class AwsS3Cleaner implements Runnable {

    private AwsS3BackgroundJob backgroundJob;
    public AwsS3Cleaner (String region, String accessKeyId, String secretAccessKey, String dynamoTable, String s3Bucket) {
	backgroundJob = new AwsS3BackgroundJob(region, accessKeyId, secretAccessKey, dynamoTable, s3Bucket);
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

