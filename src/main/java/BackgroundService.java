import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ResourceBundle;
import java.util.MissingResourceException;

public class BackgroundService {
    private static final Logger logger_ = LoggerFactory.getLogger(BackgroundService.class);

    public static void main (String [] args) {

	try {
//	    ResourceBundle bundle = ResourceBundle.getBundle("zipnshare",Locale.getDefault(),cl);
	    ResourceBundle bundle = ResourceBundle.getBundle("zipnshare");
	    int maxFileCount = Integer.valueOf(bundle.getString("zipnshare.maxFileCount"));
	    long maxFileSize = Long.valueOf(bundle.getString("zipnshare.maxFileSize"));
	    boolean useZipConverter = Boolean.valueOf(bundle.getString("zipnshare.useZipConverter"));
	    String storageType = bundle.getString("zipnshare.storageType");
	    if (storageType.equals("azureBlobV8")) {
		String cosmosAccountEndpoint = bundle.getString("zipnshare.cosmosAccountEndpoint");
		String cosmosAccountKey = bundle.getString("zipnshare.cosmosAccountKey");
		String cosmosDatabase = bundle.getString("zipnshare.cosmosDatabase");
		String storageAccountCS = bundle.getString("zipnshare.storageAccountCS");
		String cloudBlobContainer = bundle.getString("zipnshare.cloudBlobContainer");
		String queueName = bundle.getString("zipnshare.queueName");
	    } else if (storageType.equals("azureBlobV12")) {
		String cosmosAccountEndpoint = bundle.getString("zipnshare.cosmosAccountEndpoint");
		String cosmosAccountKey = bundle.getString("zipnshare.cosmosAccountKey");
		String cosmosDatabase = bundle.getString("zipnshare.cosmosDatabase");
		String storageAccountCS = bundle.getString("zipnshare.storageAccountCS");
		String blobServiceContainer = bundle.getString("zipnshare.blobServiceContainer");
		String queueName = bundle.getString("zipnshare.queueName");
	    } else if (storageType.equals("awsS3")) {
		String region = bundle.getString("zipnshare.awsRegion");
		String accessKeyId = bundle.getString("zipnshare.awsAccessKeyId");
		String secretAccessKey = bundle.getString("zipnshare.awsSecretAccessKey");
		String dynamoTable = bundle.getString("zipnshare.dynamoTable");
		String s3Bucket = bundle.getString("zipnshare.s3Bucket");
		String sqsUrl = bundle.getString("zipnshare.sqsUrl");
		String sqsGroupId = bundle.getString("zipnshare.sqsGroupId");
	    } else {
		logger_.error("invalid storageType");
	    }
	} catch (MissingResourceException ex) {
	    logger_.error("failed to init dataStorage.", ex);
	} catch (Exception ex) {
	    logger_.error("failed to init dataStorage.", ex);
	}
	
    }

}
