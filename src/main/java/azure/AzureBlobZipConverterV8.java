package azure;

import java.util.List;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import com.azure.cosmos.*;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.queue.*;

import org.mitsuji.vswf.ZipWriter;
import type.FileListItem;

public class AzureBlobZipConverterV8 implements Runnable {

    private CosmosClient cosmosClient;
    private String cosmosDatabase;
    private CloudBlobClient cloudBlobClient;
    private String cloudBlobContainer;
    private CloudQueueClient cloudQueueClient;
    private String queueName;
    public AzureBlobZipConverterV8 (String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String cloudBlobContainer, String queueName) throws Exception {
	cosmosClient = new CosmosClientBuilder()
	    .endpoint(cosmosAccountEndpoint).key(cosmosAccountKey).buildClient();
	this.cosmosDatabase = cosmosDatabase;
	try {
	    cloudBlobClient = CloudStorageAccount.parse(storageAccountCS).createCloudBlobClient();
	} catch (URISyntaxException | InvalidKeyException ex) {
	    throw new Exception("failed to create cloudBlobClient", ex);
	}
	this.cloudBlobContainer = cloudBlobContainer;
	try {
	    cloudQueueClient = CloudStorageAccount.parse(storageAccountCS).createCloudQueueClient();
	} catch (URISyntaxException | InvalidKeyException ex) {
	    throw new Exception("failed to create cloudQueueClient", ex);
	}
	this.queueName = queueName;
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
		    DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
		    BlobManagerV8 bm = new BlobManagerV8 (cloudBlobClient,cloudBlobContainer,sessionKey);
		    try {
			// [TODO] zip password
			ZipWriter zw = new ZipWriter(bm.getZipOutputStream());
			List<FileListItem> files = dm.getFileList();
			for (int i = 0; i < files.size(); i++) {
			    FileListItem file = files.get(i);
			    zw.append(file.fileName,bm.getFileDataInputStream(i));
			}
			zw.close();
			dm.zip();
			queue.deleteMessage(message);
		    } catch (IOException ex) {
			// [TODO] log
		    }
		}
	    } catch (URISyntaxException | StorageException ex) {
// [TODO] log
//		throw new DataStorageException("failed to lockSession: queue.addMessage",ex);
	    }
	    try {
		Thread.sleep (500);
	    } catch (InterruptedException ex) {
		break;
	    }
	}
    }

}

