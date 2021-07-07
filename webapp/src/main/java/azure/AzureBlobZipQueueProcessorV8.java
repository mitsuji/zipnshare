package azure;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.queue.*;

import type.BackgroundJob;
import type.ZipQueueProcessor;

public class AzureBlobZipQueueProcessorV8 implements ZipQueueProcessor {

    private BackgroundJob backgroundJob;
    private CloudQueueClient cloudQueueClient;
    private String queueName;
    public AzureBlobZipQueueProcessorV8 (String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String cloudBlobContainer,
					 String queueName) throws Exception {
	backgroundJob = new AzureBlobBackgroundJobV8(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase,  storageAccountCS, cloudBlobContainer);
	try {
	    cloudQueueClient = CloudStorageAccount.parse(storageAccountCS).createCloudQueueClient();
	} catch (URISyntaxException | InvalidKeyException ex) {
	    throw new Exception("failed to create cloudQueueClient", ex);
	}
	this.queueName = queueName;
    }

    public void process () throws Exception {
	CloudQueue queue = cloudQueueClient.getQueueReference(queueName);
	CloudQueueMessage message = queue.retrieveMessage();
	if (message != null) {
//	    System.out.println("messageBody: " + message.getMessageContentAsString());
//	    System.out.println("messageId: " + message.getMessageId());
//	    System.out.println("popReceipt: " + message.getPopReceipt());
	    String sessionKey = message.getMessageContentAsString();

	    // [MEMO] needs extend visibility timeout ?
	
	    boolean succeed = false;
	    try {
		backgroundJob.zipConvert(sessionKey);
		succeed = true;
	    } catch (BackgroundJob.BackgroundJobException ex ) {
		// [TODO] log
		ex.printStackTrace();
	    }

	    if (succeed) {
		queue.deleteMessage(message);
	    }
	}
    }

}

