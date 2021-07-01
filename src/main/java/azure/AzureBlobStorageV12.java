package azure;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import com.azure.cosmos.*;

import com.azure.storage.blob.*;
import com.azure.storage.queue.*;
import com.azure.storage.queue.models.*;

import org.mitsuji.vswf.Util;
import type.FileListItem;
import type.DataStorage;

public class AzureBlobStorageV12 implements DataStorage {

    private CosmosClient cosmosClient;
    private String cosmosDatabase;
    private BlobServiceClient blobServiceClient;
    private String blobServiceContainer;
    private QueueServiceClient queueServiceClient;
    private String queueName;
    private int maxFileCount;
    private long maxFileSize;
    private boolean useZipConverter;
    public AzureBlobStorageV12 (String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase, String storageAccountCS, String blobServiceContainer, String queueName, int maxFileCount, long maxFileSize, boolean useZipConverter) {
	cosmosClient  = new CosmosClientBuilder()
	    .endpoint(cosmosAccountEndpoint).key(cosmosAccountKey).buildClient();
	this.cosmosDatabase = cosmosDatabase;
	blobServiceClient = new BlobServiceClientBuilder()
	    .connectionString(storageAccountCS).buildClient();
	this.blobServiceContainer = blobServiceContainer;
	if (useZipConverter) {
	    queueServiceClient = new QueueServiceClientBuilder()
		.connectionString(storageAccountCS)
		.buildClient();
	    this.queueName = queueName;
	}
	this.maxFileCount = maxFileCount;
	this.maxFileSize = maxFileSize;
	this.useZipConverter = useZipConverter;
    }

    public void init() {
	DatabaseManager.createContainerIfNotExists (cosmosClient,cosmosDatabase);
    }

    public void destroy() {
	cosmosClient.close();
    }
    
    public String createSession () throws DataStorageException {
	String sessionKey = Util.genAlphaNumericKey(16);
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	dm.create();
	return sessionKey;
    }
    public void setOwnerKey (String sessionKey, String ownerKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to setOwnerKey: invalid session key");
	}
	dm.setOwnerKey(ownerKey);
    }
    public String createFileData (String sessionKey, String fileName, String contentType) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	BlobManagerV12 bm = new BlobManagerV12 (blobServiceClient,blobServiceContainer,sessionKey);
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
    }
    public void upload (String sessionKey, String fileId, InputStream in, long len) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	BlobManagerV12 bm = new BlobManagerV12 (blobServiceClient,blobServiceContainer,sessionKey);
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
	try {
	    bm.upload(Integer.valueOf(fileId),in,len);
	} catch (IOException ex) {
	    throw new DataStorageException("failed to upload",ex);
	}
    }
    public void closeFileData (String sessionKey, String fileId) throws DataStorageException {
	// do nothing
    }
    public void lockSession (String sessionKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to lockSession: invalid session key");
	}
	if(dm.locked()) {
	    throw new DataStorageException("failed to lockSession: session already locked");
	}
	dm.lock();
	if (useZipConverter) {
	    QueueClient queueClient = queueServiceClient.getQueueClient(queueName);
	    queueClient.sendMessage(sessionKey);
	}
    }

    public boolean hasLocked (String sessionKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to hasLocked: invalid session key");
	}
	return dm.locked();
    }
    public long getFileSize (String sessionKey, String fileId) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	BlobManagerV12 bm = new BlobManagerV12 (blobServiceClient,blobServiceContainer,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to getFileSize: invalid session key");
	}
	if(!dm.hasFile(Integer.valueOf(fileId))) {
	    throw new NoSuchFileDataException("failed to getFileSize: invalid fileId");
	}
	return bm.getFileSize(Integer.valueOf(fileId));
    }
    public List<FileListItem> getFileList(String sessionKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to getFileList: invalid session key");
	}
	return dm.getFileList();
    }
    public FileListItem getFileInfo(String sessionKey, String fileId) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to getFileInfo: invalid session key");
	}
	if(!dm.hasFile(Integer.valueOf(fileId))) {
	    throw new NoSuchFileDataException("failed to getFileInfo: invalid fileId");
	}
	return dm.getFileInfo(Integer.valueOf(fileId));
    }

    public void download (String sessionKey, String fileId, OutputStream out) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	BlobManagerV12 bm = new BlobManagerV12 (blobServiceClient,blobServiceContainer,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to download: invalid session key");
	}
	if(!dm.hasFile(Integer.valueOf(fileId))) {
	    throw new NoSuchFileDataException("failed to download: invalid fileId");
	}
	bm.download (Integer.valueOf(fileId),out);
    }

    public boolean matchOwnerKey (String sessionKey, String ownerKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to matchOwnerKey: invalid session key");
	}
	return dm.matchOwnerKey(ownerKey);
    }

    public void deleteSession (String sessionKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	BlobManagerV12 bm = new BlobManagerV12 (blobServiceClient,blobServiceContainer,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to deleteSession: invalid session key");
	}
	bm.deleteAll();
	dm.delete();
    }

    public boolean hasZiped (String sessionKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to hasZiped: invalid session key");
	}
	return dm.ziped();
    }
    public long getZipFileSize (String sessionKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	BlobManagerV12 bm = new BlobManagerV12 (blobServiceClient,blobServiceContainer,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to getZipFileSize: invalid session key");
	}
	if(!dm.ziped()) {
	    throw new NoSuchFileDataException("failed to getZipFileSize: invalid fileId");
	}
	return bm.getZipFileSize();
    }
    public void zipDownload (String sessionKey, OutputStream out) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager(cosmosClient,cosmosDatabase,sessionKey);
	BlobManagerV12 bm = new BlobManagerV12 (blobServiceClient,blobServiceContainer,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to zipDownload: invalid session key");
	}
	if(!dm.ziped()) {
	    throw new NoSuchFileDataException("failed to zipDownload: invalid fileId");
	}
	bm.zipDownload (out);
    }

}
