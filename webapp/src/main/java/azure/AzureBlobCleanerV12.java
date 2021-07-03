package azure;

import java.util.List;
import java.util.Iterator;

import java.io.IOException;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.*;
import com.fasterxml.jackson.databind.JsonNode;

import com.azure.storage.blob.*;

public class AzureBlobCleanerV12 implements Runnable {

    private CosmosClient cosmosClient;
    private String cosmosDatabase;
    private BlobServiceClient blobServiceClient;
    private String blobServiceContainer;
    public AzureBlobCleanerV12 (String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String blobServiceContainer) {
	cosmosClient  = new CosmosClientBuilder()
	    .endpoint(cosmosAccountEndpoint).key(cosmosAccountKey).buildClient();
	this.cosmosDatabase = cosmosDatabase;
	blobServiceClient = new BlobServiceClientBuilder()
	    .connectionString(storageAccountCS).buildClient();
	this.blobServiceContainer = blobServiceContainer;
    }

    public void run () {
	while (true) {
	    CosmosContainer container = cosmosClient.getDatabase(cosmosDatabase).getContainer("sessions");
	    String sql = "SELECT c.id FROM c";
	    CosmosPagedIterable<JsonNode> res = container.queryItems(sql, new CosmosQueryRequestOptions(), JsonNode.class);
	    Iterator<JsonNode> it = res.iterator();
	    while(it.hasNext()) {
		JsonNode node = it.next();
		String sessionKey = node.get("id").asText();
		DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
		BlobManagerV12 bm = new BlobManagerV12 (blobServiceClient,blobServiceContainer,sessionKey);
		long now = System.currentTimeMillis();
		long createdAt = dm.getCreatedAt();
		boolean locked = dm.locked();
		boolean expired1 = createdAt + (7 * 24 * 60 * 60 * 1000) < now; // expired
//		boolean expired1 = createdAt + (10 * 60 * 1000) < now; // expired (test)
		boolean expired2 = (!locked) && (createdAt + (1 * 60 * 60 * 1000) < now); // gabage
		if (expired1 || expired2) {
		    bm.deleteAll();
		    dm.delete();
		}
	    }
	    try {
		Thread.sleep (60 * 1000);
	    } catch (InterruptedException ex) {
		break;
	    }
	}
    }

}

