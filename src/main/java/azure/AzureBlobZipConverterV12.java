package azure;

import java.util.List;

import java.io.IOException;

import com.azure.cosmos.*;

import com.azure.storage.blob.*;
import com.azure.storage.queue.*;
import com.azure.storage.queue.models.*;

import org.mitsuji.vswf.ZipWriter;
import type.FileListItem;

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
//		System.out.println("messageBody: " + item.getBody().toString());
//		System.out.println("messageId: " + item.getMessageId());
//		System.out.println("popReceipt: " + item.getPopReceipt());
		String sessionKey = item.getBody().toString();
		DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
		BlobManagerV12 bm = new BlobManagerV12 (blobServiceClient,blobServiceContainer,sessionKey);
		try {
		    // [TODO] zip password
		    ZipWriter zw = new ZipWriter(bm.getZipOutputStream());
		    List<FileListItem> files = dm.getFileList();
		    for (int i = 0; i < files.size(); i++) {
			FileListItem file = files.get(i);
			zw.append(file.fileName,bm.getFileInputStream(i));
		    }
		    zw.close();
		    dm.zip();
		    queueClient.deleteMessage(item.getMessageId(),item.getPopReceipt());
		} catch (IOException ex) {
		    // [TODO] log
		}
	    }
	    try {
		Thread.sleep (500);
	    } catch (InterruptedException ex) {
		break;
	    }
	}
    }

}

