package azure;

import java.io.IOException;

import com.azure.storage.queue.*;
import com.azure.storage.queue.models.*;

public class AzureBlobZipConverterV12 implements Runnable {

    private QueueServiceClient queueServiceClient;
    private String queueName;
    private AzureBlobBackgroundJobV12 backgroundJob;
    public AzureBlobZipConverterV12 (String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String blobServiceContainer, String queueName) {
	queueServiceClient = new QueueServiceClientBuilder()
	    .connectionString(storageAccountCS)
	    .buildClient();
	this.queueName = queueName;
	backgroundJob = new AzureBlobBackgroundJobV12(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase, storageAccountCS, blobServiceContainer);
    }

    public void run () {
	while (true) {
	    try {
		QueueClient queueClient = queueServiceClient.getQueueClient(queueName);
		QueueMessageItem item = queueClient.receiveMessage();
		if (item != null) {
//		    System.out.println("messageBody: " + item.getBody().toString());
//		    System.out.println("messageId: " + item.getMessageId());
//		    System.out.println("popReceipt: " + item.getPopReceipt());
		    String sessionKey = item.getBody().toString();

		    // [MEMO] needs extend visibility timeout ?
		
		    boolean succeed = false;
		    try {
			backgroundJob.zipConvert(sessionKey);
			succeed = true;
		    } catch (IOException ex ) {
			// [TODO] log
		    }

		    if (succeed) {
			queueClient.deleteMessage(item.getMessageId(),item.getPopReceipt());
		    }
		}
		Thread.sleep (500);
	    } catch (InterruptedException ex) {
		break;
	    } catch (Exception ex) {
		// [TODO] log
	    }
	}
    }

}

