import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ResourceBundle;
import java.util.MissingResourceException;

import azure.AzureBlobZipConverterV12;
import azure.AzureBlobCleanerV12;
import azure.AzureBlobZipConverterV8;
import aws.AwsS3ZipConverter;
import aws.AwsS3Cleaner;

public class BackgroundService {
    private static final Logger logger_ = LoggerFactory.getLogger(BackgroundService.class);

    // [TODO] launch Cleaner
    public static void main (String [] args) {

	try {
	    ResourceBundle bundle = ResourceBundle.getBundle("zipnshare");
	    boolean useZipConverter = Boolean.valueOf(bundle.getString("zipnshare.useZipConverter"));
	    if (!useZipConverter) {
		logger_.info("terminate due to zip converter disabled");
		return;
	    }
	    Runnable zipConverter;
	    String storageType = bundle.getString("zipnshare.storageType");
	    if (storageType.equals("localFile")) {
		logger_.info("terminate due to storage type localFile");
		return;
	    } else if (storageType.equals("azureBlobV8")) {
		String cosmosAccountEndpoint = bundle.getString("zipnshare.cosmosAccountEndpoint");
		String cosmosAccountKey = bundle.getString("zipnshare.cosmosAccountKey");
		String cosmosDatabase = bundle.getString("zipnshare.cosmosDatabase");
		String storageAccountCS = bundle.getString("zipnshare.storageAccountCS");
		String cloudBlobContainer = bundle.getString("zipnshare.cloudBlobContainer");
		String queueName = bundle.getString("zipnshare.queueName");
		zipConverter = new AzureBlobZipConverterV8(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase, storageAccountCS, cloudBlobContainer, queueName);
	    } else if (storageType.equals("azureBlobV12")) {
		String cosmosAccountEndpoint = bundle.getString("zipnshare.cosmosAccountEndpoint");
		String cosmosAccountKey = bundle.getString("zipnshare.cosmosAccountKey");
		String cosmosDatabase = bundle.getString("zipnshare.cosmosDatabase");
		String storageAccountCS = bundle.getString("zipnshare.storageAccountCS");
		String blobServiceContainer = bundle.getString("zipnshare.blobServiceContainer");
		String queueName = bundle.getString("zipnshare.queueName");
		zipConverter = new AzureBlobZipConverterV12(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase, storageAccountCS, blobServiceContainer, queueName);

		Runnable cleaner = new AzureBlobCleanerV12(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase, storageAccountCS, blobServiceContainer);
		Thread cleanerThread = new Thread(cleaner);
		cleanerThread.start();
	    } else if (storageType.equals("awsS3")) {
		String region = bundle.getString("zipnshare.awsRegion");
		String accessKeyId = bundle.getString("zipnshare.awsAccessKeyId");
		String secretAccessKey = bundle.getString("zipnshare.awsSecretAccessKey");
		String dynamoTable = bundle.getString("zipnshare.dynamoTable");
		String s3Bucket = bundle.getString("zipnshare.s3Bucket");
		String sqsUrl = bundle.getString("zipnshare.sqsUrl");
		String sqsGroupId = bundle.getString("zipnshare.sqsGroupId");
		zipConverter = new AwsS3ZipConverter(region, accessKeyId, secretAccessKey, dynamoTable, s3Bucket, sqsUrl, sqsGroupId);

		Runnable cleaner = new AwsS3Cleaner(region, accessKeyId, secretAccessKey, dynamoTable, s3Bucket);
		Thread cleanerThread = new Thread(cleaner);
		cleanerThread.start();
	    } else {
		logger_.error("invalid storageType");
		return;
	    }
	    
	    Thread zipConverterThread = new Thread(zipConverter);
	    zipConverterThread.start();
	    
	} catch (MissingResourceException ex) {
	    logger_.error("failed to init dataStorage.", ex);
	} catch (Exception ex) {
	    logger_.error("failed to init dataStorage.", ex);
	}
	
    }

}
