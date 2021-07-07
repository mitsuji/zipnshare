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

import org.mitsuji.vswf.ZipWriter;
import type.FileListItem;
import type.BackgroundJob;

public class AzureBlobBackgroundJobV8 implements BackgroundJob {

    private CosmosClient cosmosClient;
    private String cosmosDatabase;
    private CloudBlobClient cloudBlobClient;
    private String cloudBlobContainer;

    // for clean ()
    private long cleanExpiredSeconds;
    private long cleanGarbageSeconds;
    public AzureBlobBackgroundJobV8 (String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String cloudBlobContainer) throws Exception {
	this (cosmosAccountEndpoint,cosmosAccountKey,cosmosDatabase,storageAccountCS,cloudBlobContainer,0,0);
    }

    public AzureBlobBackgroundJobV8 (String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String cloudBlobContainer,
				     long cleanExpiredSeconds, long cleanGarbageSeconds) throws Exception {
	cosmosClient = new CosmosClientBuilder()
	    .endpoint(cosmosAccountEndpoint).key(cosmosAccountKey).buildClient();
	this.cosmosDatabase = cosmosDatabase;
	try {
	    cloudBlobClient = CloudStorageAccount.parse(storageAccountCS).createCloudBlobClient();
	} catch (URISyntaxException | InvalidKeyException ex) {
	    throw new Exception("failed to create cloudBlobClient", ex);
	}
	this.cloudBlobContainer = cloudBlobContainer;
	this.cleanExpiredSeconds = cleanExpiredSeconds;
	this.cleanGarbageSeconds = cleanGarbageSeconds;
    }

    public void clean () throws BackgroundJobException {
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
		boolean expired1 = createdAt + (cleanExpiredSeconds * 1000) < now; // expired
		boolean expired2 = (!locked) && (createdAt + (cleanGarbageSeconds * 1000) < now); // garbage
		if (expired1 || expired2) {
		    bm.deleteAll();
		    dm.delete();
		}
	    }
	} catch (URISyntaxException | StorageException ex) {
	    throw new BackgroundJobException ("failed to clean", ex);
	}
    }

    public void zipConvert (String sessionKey) throws BackgroundJobException {
	try {
	    DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	    BlobManagerV8 bm = new BlobManagerV8 (cloudBlobClient,cloudBlobContainer,sessionKey);

	    // [TODO] zip password
	    ZipWriter zw = new ZipWriter(bm.getZipOutputStream());
	    List<FileListItem> files = dm.getFileList();
	    for (int i = 0; i < files.size(); i++) {
		FileListItem file = files.get(i);
		zw.append(file.fileName,bm.getFileDataInputStream(i));
	    }
	    zw.close();
	    dm.zip();
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new BackgroundJobException ("failed to zipConvert", ex);
	}
    }

}

