import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ResourceBundle;
import java.util.MissingResourceException;

import azure.AzureBlobBackgroundJobV12;
import azure.AzureBlobBackgroundJobV8;
import aws.AwsS3BackgroundJob;
import azure.AzureBlobZipQueueProcessorV12;
import azure.AzureBlobZipQueueProcessorV8;
import aws.AwsS3ZipQueueProcessor;

import type.BackgroundJob;
import type.ZipQueueProcessor;

public class BackgroundService {
    private static final Logger logger_ = LoggerFactory.getLogger(BackgroundService.class);

    public static void main (String [] args) {

	try {
	    ResourceBundle bundle = ResourceBundle.getBundle("zipnshare");
	    boolean useZipConverter = Boolean.valueOf(bundle.getString("zipnshare.useZipConverter"));
	    if (!useZipConverter) {
		logger_.info("terminate due to zip converter disabled");
		return;
	    }
	    String storageType = bundle.getString("zipnshare.storageType");
	    long cleanIntervalSeconds = Long.valueOf(bundle.getString("zipnshare.cleanIntervalSeconds"));
	    long cleanExpiredSeconds = Long.valueOf(bundle.getString("zipnshare.cleanExpiredSeconds"));
	    long cleanGarbageSeconds = Long.valueOf(bundle.getString("zipnshare.cleanGarbageSeconds"));
	    long zipConvertIntervalSeconds = Long.valueOf(bundle.getString("zipnshare.zipConvertIntervalSeconds"));
	    BackgroundJob cleanerBackgroundJob;
	    ZipQueueProcessor zipQueueProcessor;
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
		zipQueueProcessor = new AzureBlobZipQueueProcessorV8(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase, storageAccountCS, cloudBlobContainer, queueName);
		cleanerBackgroundJob = new AzureBlobBackgroundJobV8(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase, storageAccountCS, cloudBlobContainer,
								    cleanExpiredSeconds, cleanGarbageSeconds);
	    } else if (storageType.equals("azureBlobV12")) {
		String cosmosAccountEndpoint = bundle.getString("zipnshare.cosmosAccountEndpoint");
		String cosmosAccountKey = bundle.getString("zipnshare.cosmosAccountKey");
		String cosmosDatabase = bundle.getString("zipnshare.cosmosDatabase");
		String storageAccountCS = bundle.getString("zipnshare.storageAccountCS");
		String blobServiceContainer = bundle.getString("zipnshare.blobServiceContainer");
		String queueName = bundle.getString("zipnshare.queueName");
		zipQueueProcessor = new AzureBlobZipQueueProcessorV12(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase, storageAccountCS, blobServiceContainer, queueName);
		cleanerBackgroundJob = new AzureBlobBackgroundJobV12(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase, storageAccountCS, blobServiceContainer,
								     cleanExpiredSeconds, cleanGarbageSeconds);
	    } else if (storageType.equals("awsS3")) {
		String region = bundle.getString("zipnshare.awsRegion");
		String accessKeyId = bundle.getString("zipnshare.awsAccessKeyId");
		String secretAccessKey = bundle.getString("zipnshare.awsSecretAccessKey");
		String dynamoTable = bundle.getString("zipnshare.dynamoTable");
		String s3Bucket = bundle.getString("zipnshare.s3Bucket");
		String sqsUrl = bundle.getString("zipnshare.sqsUrl");
		String sqsGroupId = bundle.getString("zipnshare.sqsGroupId");
		zipQueueProcessor = new AwsS3ZipQueueProcessor(region, accessKeyId, secretAccessKey, dynamoTable, s3Bucket, sqsUrl, sqsGroupId);
		cleanerBackgroundJob = new AwsS3BackgroundJob(region, accessKeyId, secretAccessKey, dynamoTable, s3Bucket,
							      cleanExpiredSeconds, cleanGarbageSeconds);
	    } else {
		logger_.error("terminate due to invalid storageType");
		return;
	    }

	    Thread cleanerThread = new Thread(new Cleaner(cleanIntervalSeconds, cleanerBackgroundJob));
	    Thread zipConverterThread = new Thread(new ZipConverter(zipConvertIntervalSeconds, zipQueueProcessor));
	    cleanerThread.start();
	    zipConverterThread.start();
	    
	} catch (MissingResourceException ex) {
	    logger_.error("failed to init BackgroundService.", ex);
	} catch (Exception ex) {
	    logger_.error("failed to init BackgroundService.", ex);
	}
	
    }

}
