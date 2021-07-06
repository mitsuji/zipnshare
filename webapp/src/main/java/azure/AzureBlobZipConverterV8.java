package azure;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.queue.*;

public class AzureBlobZipConverterV8 implements Runnable {

    private long zipConvertIntervalSeconds;
    private CloudQueueClient cloudQueueClient;
    private String queueName;
    private AzureBlobBackgroundJobV8 backgroundJob;
    public AzureBlobZipConverterV8 (long zipConvertIntervalSeconds,
				    String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String cloudBlobContainer, String queueName) throws Exception {
	this.zipConvertIntervalSeconds = zipConvertIntervalSeconds;
	try {
	    cloudQueueClient = CloudStorageAccount.parse(storageAccountCS).createCloudQueueClient();
	} catch (URISyntaxException | InvalidKeyException ex) {
	    throw new Exception("failed to create cloudQueueClient", ex);
	}
	this.queueName = queueName;
	backgroundJob = new AzureBlobBackgroundJobV8(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase,  storageAccountCS, cloudBlobContainer);
    }

    public void run () {
	while (true) {
	    try {
		CloudQueue queue = cloudQueueClient.getQueueReference(queueName);
		CloudQueueMessage message = queue.retrieveMessage();
		if (message != null) {
//		    System.out.println("messageBody: " + message.getMessageContentAsString());
//		    System.out.println("messageId: " + message.getMessageId());
//		    System.out.println("popReceipt: " + message.getPopReceipt());
		    String sessionKey = message.getMessageContentAsString();

		    // [MEMO] needs extend visibility timeout ?
		
		    boolean succeed = false;
		    try {
			backgroundJob.zipConvert(sessionKey);
			succeed = true;
		    } catch (URISyntaxException | StorageException | IOException ex ) {
			// [TODO] log
			ex.printStackTrace();
		    }

		    if (succeed) {
			queue.deleteMessage(message);
		    }
		}
		Thread.sleep (zipConvertIntervalSeconds * 1000);
	    } catch (InterruptedException ex) {
		break;
	    } catch (Exception ex) {
		// [TODO] log
		ex.printStackTrace();
	    }
	}
    }

}

