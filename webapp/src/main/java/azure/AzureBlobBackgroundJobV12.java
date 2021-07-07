package azure;

import java.util.List;
import java.util.Iterator;

import java.io.IOException;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.*;

import com.azure.storage.blob.*;
import com.fasterxml.jackson.databind.JsonNode;

import org.mitsuji.vswf.ZipWriter;
import type.FileListItem;
import type.BackgroundJob;

public class AzureBlobBackgroundJobV12 implements BackgroundJob {

    private CosmosClient cosmosClient;
    private String cosmosDatabase;
    private BlobServiceClient blobServiceClient;
    private String blobServiceContainer;

    // for clean ()
    private long cleanExpiredSeconds;
    private long cleanGarbageSeconds;
    public AzureBlobBackgroundJobV12 (String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String blobServiceContainer) {
	this(cosmosAccountEndpoint,cosmosAccountKey,cosmosDatabase,storageAccountCS,blobServiceContainer,0,0);
    }

    public AzureBlobBackgroundJobV12 (String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String blobServiceContainer,
				      long cleanExpiredSeconds, long cleanGarbageSeconds) {
	cosmosClient  = new CosmosClientBuilder()
	    .endpoint(cosmosAccountEndpoint).key(cosmosAccountKey).buildClient();
	this.cosmosDatabase = cosmosDatabase;
	blobServiceClient = new BlobServiceClientBuilder()
	    .connectionString(storageAccountCS).buildClient();
	this.blobServiceContainer = blobServiceContainer;
	this.cleanExpiredSeconds = cleanExpiredSeconds;
	this.cleanGarbageSeconds = cleanGarbageSeconds;
    }

    public void clean () throws BackgroundJobException {
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
	    boolean expired1 = createdAt + (cleanExpiredSeconds * 1000) < now; // expired
	    boolean expired2 = (!locked) && (createdAt + (cleanGarbageSeconds * 1000) < now); // gabage
	    if (expired1 || expired2) {
		bm.deleteAll();
		dm.delete();
	    }
	}
    }

    public void zipConvert (String sessionKey) throws BackgroundJobException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	BlobManagerV12 bm = new BlobManagerV12 (blobServiceClient,blobServiceContainer,sessionKey);

	if (!dm.exists()) {
	    throw new BackgroundJob.NoSuchSessionException ("failed to zipConvert: session missing");
	}
	
	try {
	    // [TODO] zip password
	    ZipWriter zw = new ZipWriter(bm.getZipOutputStream());
	    List<FileListItem> files = dm.getFileList();
	    for (int i = 0; i < files.size(); i++) {
		FileListItem file = files.get(i);
		zw.append(file.fileName,bm.getFileDataInputStream(i));
	    }
	    zw.close();
	} catch (IOException ex) {
	    throw new BackgroundJobException ("failed to zipConvert", ex);
	}
	dm.zip();
    }

}
