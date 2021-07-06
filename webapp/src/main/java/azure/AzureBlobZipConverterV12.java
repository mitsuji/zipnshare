package azure;

import java.io.IOException;

import com.azure.storage.queue.*;
import com.azure.storage.queue.models.*;

import java.util.Base64;

public class AzureBlobZipConverterV12 implements Runnable {

    private long zipConvertIntervalSeconds;
    private QueueServiceClient queueServiceClient;
    private String queueName;
    private AzureBlobBackgroundJobV12 backgroundJob;
    public AzureBlobZipConverterV12 (long zipConvertIntervalSeconds,
				     String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String blobServiceContainer, String queueName) {
	this.zipConvertIntervalSeconds = zipConvertIntervalSeconds;
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
		    String sessionKeyBE64 = item.getBody().toString();
		    String sessionKey = new String (Base64.getDecoder().decode(sessionKeyBE64), "UTF-8");

		    // [MEMO] needs extend visibility timeout ?
		
		    boolean succeed = false;
		    try {
			backgroundJob.zipConvert(sessionKey);
			succeed = true;
		    } catch (IOException ex ) {
			// [TODO] log
			ex.printStackTrace();
		    }

		    if (succeed) {
			queueClient.deleteMessage(item.getMessageId(),item.getPopReceipt());
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

