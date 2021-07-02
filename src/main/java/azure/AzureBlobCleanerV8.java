package azure;

import java.util.List;
import java.util.Iterator;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.*;
import com.fasterxml.jackson.databind.JsonNode;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

public class AzureBlobCleanerV8 implements Runnable {

    private CosmosClient cosmosClient;
    private String cosmosDatabase;
    private CloudBlobClient cloudBlobClient;
    private String cloudBlobContainer;
    public AzureBlobCleanerV8 (String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String cloudBlobContainer) throws Exception{
	cosmosClient  = new CosmosClientBuilder()
	    .endpoint(cosmosAccountEndpoint).key(cosmosAccountKey).buildClient();
	this.cosmosDatabase = cosmosDatabase;
	try {
	    cloudBlobClient = CloudStorageAccount.parse(storageAccountCS).createCloudBlobClient();
	} catch (URISyntaxException | InvalidKeyException ex) {
	    throw new Exception("failed to create cloudBlobClient", ex);
	}
	this.cloudBlobContainer = cloudBlobContainer;
    }

    public void run () {
	while (true) {
	    try {
		CosmosContainer container = cosmosClient.getDatabase(cosmosDatabase).getContainer("sessions");
		String sql = "SELECT c.id FROM c";
		CosmosPagedIterable<JsonNode> res = container.queryItems(sql, new CosmosQueryRequestOptions(), JsonNode.class);
		Iterator<JsonNode> it = res.iterator();
		while(it.hasNext()) {
		    JsonNode node = it.next();
		    String sessionKey = node.get("id").asText();
		    DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
		    BlobManagerV8 bm = new BlobManagerV8 (cloudBlobClient,cloudBlobContainer,sessionKey);
		    long now = System.currentTimeMillis();
		    long createdAt = dm.getCreatedAt();
		    boolean locked = dm.locked();
		    boolean expired1 = createdAt + (7 * 24 * 60 * 60 * 1000) < now; // expired
//		    boolean expired1 = createdAt + (10 * 60 * 1000) < now; // expired (test)
		    boolean expired2 = (!locked) && (createdAt + (1 * 60 * 60 * 1000) < now); // gabage
		    if (expired1 || expired2) {
			bm.deleteAll();
			dm.delete();
		    }
		}
	    } catch (URISyntaxException | StorageException ex) {
// [TODO] log
//		throw new DataStorageException("failed to lockSession: queue.addMessage",ex);
	    }
	    try {
		Thread.sleep (60 * 1000);
	    } catch (InterruptedException ex) {
		break;
	    }
	}
    }

}
