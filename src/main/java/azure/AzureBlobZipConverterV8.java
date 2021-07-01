package azure;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import com.azure.cosmos.*;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.queue.*;

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
	    System.out.printf("AzureBlobZipConverterV8\n");
	    try {
		Thread.sleep (500);
	    } catch (InterruptedException ex) {
		break;
	    }
	}
    }

}

