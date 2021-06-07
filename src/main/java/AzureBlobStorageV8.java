import java.util.List;
import java.util.ArrayList;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosPatchOperations;

import reactor.core.publisher.Mono;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosPatchItemRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.CosmosItemIdentity;

import com.azure.cosmos.BulkOperations;
import com.azure.cosmos.CosmosItemOperation;
import com.azure.cosmos.CosmosBulkOperationResponse;

import org.mitsuji.vswf.Util;

public class AzureBlobStorageV8 implements ZipnshareServlet.DataStorage {

    private static class BlobManager {
	private CloudBlobClient blobClient;
	private String container;
	private String sessionKey;
	public BlobManager (CloudBlobClient blobClient, String container, String sessionKey) {
	    this.blobClient = blobClient;
	    this.container = container;
	    this.sessionKey = sessionKey;
	}

	private String getFileDataFilePath(int fileId) {
	    return sessionKey + "/" + Integer.toString(fileId);
	}

	private CloudBlobContainer getBlobContainer() throws URISyntaxException, StorageException {
	    return blobClient.getContainerReference(container);
	}

	public void appendFileData(int fileId) throws URISyntaxException, StorageException, IOException {
	    // create file data file
	    CloudBlobContainer container = getBlobContainer();
	    CloudAppendBlob fileDataBlob = container.getAppendBlobReference(getFileDataFilePath(fileId));
	    fileDataBlob.createOrReplace();
	}
	public void upload (int fileId, InputStream in, long len) throws URISyntaxException, StorageException, IOException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudAppendBlob fileDataBlob = container.getAppendBlobReference(getFileDataFilePath(fileId));
	    fileDataBlob.append(in,len);
	}
	public void download (int fileId, OutputStream out) throws URISyntaxException, StorageException, IOException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudAppendBlob blob = container.getAppendBlobReference(getFileDataFilePath(fileId));
	    blob.download(out);
	}
	public long getFileSize(int fileId) throws URISyntaxException, StorageException, IOException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudBlob blob = container.getAppendBlobReference(getFileDataFilePath(fileId));
	    blob.downloadAttributes();
	    return blob.getProperties().getLength();
	}
	public void deleteAll() throws  URISyntaxException, StorageException, IOException {
	    ByteArrayOutputStream out = new ByteArrayOutputStream ();
	    CloudBlobContainer container = getBlobContainer();
	    for(ListBlobItem item: container.listBlobs(sessionKey + "/")) {
		if (item instanceof CloudBlob) {
		    CloudBlob blob = (CloudBlob)item;
		    blob.delete();
		}
	    }
	}

    }

    private CosmosClient cosmosClient;
    private String cosmosDatabase;
    private CloudBlobClient cloudBlobClient;
    private String cloudBlobContainer;
    private int maxFileCount;
    private long maxFileSize;
    public AzureBlobStorageV8 (String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String cloudBlobCS, String cloudBlobContainer, int maxFileCount, long maxFileSize) throws IOException {
	cosmosClient = new CosmosClientBuilder()
	    .endpoint(cosmosAccountEndpoint).key(cosmosAccountKey).buildClient();
	this.cosmosDatabase = cosmosDatabase;
	try {
	    cloudBlobClient = CloudStorageAccount.parse(cloudBlobCS).createCloudBlobClient();
	} catch (URISyntaxException | InvalidKeyException  ex) {
	    throw new IOException("failed to create blobClient", ex);
	}
	this.cloudBlobContainer = cloudBlobContainer;
	this.maxFileCount = maxFileCount;
	this.maxFileSize = maxFileSize;
    }

    public void init() {
	AzureBlobStorageV12.DatabaseManager.createContainerIfNotExists (cosmosClient,cosmosDatabase);
    }

    public void destroy () {
	cosmosClient.close();
    }

    public String createSession () throws DataStorageException {
	String sessionKey = Util.genAlphaNumericKey(16);
	AzureBlobStorageV12.DatabaseManager dm
	    = new AzureBlobStorageV12.DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	dm.create();
	return sessionKey;
    }
    public void setOwnerKey (String sessionKey, String ownerKey) throws DataStorageException {
	AzureBlobStorageV12.DatabaseManager dm
	    = new AzureBlobStorageV12.DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to setOwnerKey: invalid session key");
	}
	dm.setOwnerKey(ownerKey);
    }
    public String createFileData (String sessionKey, String fileName, String contentType) throws DataStorageException {
	try {
	    AzureBlobStorageV12.DatabaseManager dm
		= new AzureBlobStorageV12.DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	    BlobManager bm = new BlobManager (cloudBlobClient,cloudBlobContainer,sessionKey);
	    if(!dm.exists()) {
		throw new NoSuchSessionException("failed to crateFileData: invalid session key");
	    }
	    // check file count limit
	    int fileCount = dm.getFileCount();
	    if (fileCount >= maxFileCount) {
		throw new TooManyFilesException("fiailed to crateFileData: too many files");
	    }
	    // check fileName duplication
	    if (dm.isFileNameUsed(fileName)) {
		throw new DuplicatedFileNameException("fiailed to crateFileData: duplicated file name");
	    }
	    int fileId = dm.appendFile(fileName,contentType);
	    bm.appendFileData(fileId);
	    return Integer.toString(fileId);
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to createFileData", ex);
	}
    }
    public void upload (String sessionKey, String fileId, InputStream in, long len) throws DataStorageException {
	try {
	    AzureBlobStorageV12.DatabaseManager dm
		= new AzureBlobStorageV12.DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	    BlobManager bm = new BlobManager (cloudBlobClient,cloudBlobContainer,sessionKey);
	    if(!dm.exists()) {
		throw new NoSuchSessionException("failed to upload: invalid session key");
	    }
	    if(!dm.hasFile(Integer.valueOf(fileId))) {
		throw new NoSuchFileDataException("failed to upload: invalid fileId");
	    }
	    long fileSizeBefore = bm.getFileSize(Integer.valueOf(fileId));
	    long fileSizeAfter = fileSizeBefore + len;
	    if (fileSizeAfter > maxFileSize) {
		throw new TooLargeFileException("failed to upload: too large file");
	    }
	    bm.upload(Integer.valueOf(fileId),in,len);
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to upload", ex);
	}
    }
    public void closeFileData (String sessionKey, String fileId) throws DataStorageException {
	// do nothing
    }
    public void lockSession (String sessionKey) throws DataStorageException {
	AzureBlobStorageV12.DatabaseManager dm
	    = new AzureBlobStorageV12.DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to lockSession: invalid session key");
	}
	if(dm.locked()) {
	    throw new DataStorageException("failed to lockSession: session already locked");
	}
	dm.lock();
    }

    public boolean hasLocked (String sessionKey) throws DataStorageException {
	AzureBlobStorageV12.DatabaseManager dm
	    = new AzureBlobStorageV12.DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to hasLocked: invalid session key");
	}
	return dm.locked();
    }
    public long getFileSize (String sessionKey, String fileId) throws DataStorageException {
	try {
	    AzureBlobStorageV12.DatabaseManager dm
		= new AzureBlobStorageV12.DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	    BlobManager bm = new BlobManager (cloudBlobClient,cloudBlobContainer,sessionKey);
	    if(!dm.exists()) {
		throw new NoSuchSessionException("failed to getFileSize: invalid session key");
	    }
	    if(!dm.hasFile(Integer.valueOf(fileId))) {
		throw new NoSuchFileDataException("failed to getFileSize: invalid fileId");
	    }
	    return bm.getFileSize(Integer.valueOf(fileId));
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to getFileSize",ex);
	}
    }
    public List<FileListItem> getFileList(String sessionKey) throws DataStorageException {
	AzureBlobStorageV12.DatabaseManager dm
	    = new AzureBlobStorageV12.DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to getFileList: invalid session key");
	}
	return dm.getFileList();
    }
    public FileListItem getFileInfo(String sessionKey, String fileId) throws DataStorageException {
	AzureBlobStorageV12.DatabaseManager dm
	    = new AzureBlobStorageV12.DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to getFileInfo: invalid session key");
	}
	if(!dm.hasFile(Integer.valueOf(fileId))) {
	    throw new NoSuchFileDataException("failed to getFileInfo: invalid fileId");
	}
	return dm.getFileInfo(Integer.valueOf(fileId));
    }

    public void download (String sessionKey, String fileId, OutputStream out) throws DataStorageException {
	try {
	    AzureBlobStorageV12.DatabaseManager dm
		= new AzureBlobStorageV12.DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	    BlobManager bm = new BlobManager (cloudBlobClient,cloudBlobContainer,sessionKey);
	    if(!dm.exists()) {
		throw new NoSuchSessionException("failed to download: invalid session key");
	    }
	    if(!dm.hasFile(Integer.valueOf(fileId))) {
		throw new NoSuchFileDataException("failed to download: invalid fileId");
	    }
	    bm.download(Integer.valueOf(fileId),out);
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to download",ex);
	}
    }

    public boolean matchOwnerKey (String sessionKey, String ownerKey) throws DataStorageException {
	AzureBlobStorageV12.DatabaseManager dm
	    = new AzureBlobStorageV12.DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to matchOwnerKey: invalid session key");
	}
	return dm.matchOwnerKey(ownerKey);
    }

    public void deleteSession (String sessionKey) throws DataStorageException {
	try {
	    AzureBlobStorageV12.DatabaseManager dm
		= new AzureBlobStorageV12.DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	    BlobManager bm = new BlobManager (cloudBlobClient,cloudBlobContainer,sessionKey);
	    if(!dm.exists()) {
		throw new NoSuchSessionException("failed to deleteSession: invalid session key");
	    }
	    bm.deleteAll();
	    dm.delete();
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to deleteSession",ex);
	}

    }

}
