package azure;

import com.azure.cosmos.*;

import com.azure.storage.blob.*;
import com.azure.storage.queue.*;
import com.azure.storage.queue.models.*;

public class AzureBlobZipConverterV12 implements Runnable {

    private CosmosClient cosmosClient;
    private String cosmosDatabase;
    private BlobServiceClient blobServiceClient;
    private String blobServiceContainer;
    private QueueServiceClient queueServiceClient;
    private String queueName;
    public AzureBlobZipConverterV12 (String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String blobServiceContainer, String queueName) {
	cosmosClient  = new CosmosClientBuilder()
	    .endpoint(cosmosAccountEndpoint).key(cosmosAccountKey).buildClient();
	this.cosmosDatabase = cosmosDatabase;
	blobServiceClient = new BlobServiceClientBuilder()
	    .connectionString(storageAccountCS).buildClient();
	this.blobServiceContainer = blobServiceContainer;
	queueServiceClient = new QueueServiceClientBuilder()
	    .connectionString(storageAccountCS)
	    .buildClient();
	this.queueName = queueName;
    }

    public void run () {
	while (true) {
	    QueueClient queueClient = queueServiceClient.getQueueClient(queueName);
	    QueueMessageItem item = queueClient.receiveMessage();
	    if (item != null) {
		System.out.println("messageBody: " + item.getBody().toString());
		System.out.println("messageId: " + item.getMessageId());
		System.out.println("popReceipt: " + item.getPopReceipt());
//		queueClient.deleteMessage(item.getMessageId(),item.getPopReceipt());
	    }
	    try {
		Thread.sleep (500);
	    } catch (InterruptedException ex) {
		break;
	    }
	}
    }

}

