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
import com.azure.cosmos.models.*;

import com.azure.storage.blob.*;
import com.azure.storage.blob.specialized.*;
import com.azure.storage.blob.models.*;

import com.azure.storage.queue.*;
import com.azure.storage.queue.models.*;

import org.mitsuji.vswf.Util;

public class AzureBlobStorageV12 implements ZipnshareServlet.DataStorage {

    public static class DatabaseManager {

	private static class File {
	    public File() {
	    }
	    public String getFileName() {
		return fileName;
	    }
	    public void setFileName(String val) {
		fileName = val;
	    }
	    public String getContentType() {
		return contentType;
	    }
	    public void setContentType(String val) {
		contentType = val;
	    }
	    private String fileName;
	    private String contentType;
	}

	private static class Session {
	    public Session() {
	    }
	    public String getId() {
		return id;
	    }
	    public void setId(String val) {
		id = val;
	    }
	    public long getCreatedAt() {
		return createdAt;
	    }
	    public void setCreatedAt(long val) {
		createdAt = val;
	    }
	    public File[] getFiles() {
		return files;
	    }
	    public void setFiles(File[] val) {
		files = val;
	    }
	    public String getOwnerKey() {
		return ownerKey;
	    }
	    public void setOwnerKey(String val) {
		ownerKey = val;
	    }
	    public boolean getLocked() {
		return locked;
	    }
	    public void setLocked(boolean val) {
		locked = val;
	    }
	    public boolean getZiped() {
		return ziped;
	    }
	    public void setZiped(boolean val) {
		ziped = val;
	    }
	    private String id;
	    private long createdAt;
	    private File [] files;
	    private String ownerKey;
	    private boolean locked;
	    private boolean ziped;
	}

	private static final String containerName = "sessions";
	private CosmosClient cosmosClient;
	private String cosmosDatabase;
	private String sessionKey;
	public DatabaseManager (CosmosClient cosmosClient, String cosmosDatabase, String sessionKey ) {
	    this.cosmosClient = cosmosClient;
	    this.cosmosDatabase = cosmosDatabase;
	    this.sessionKey = sessionKey;
	}

	public static void createContainerIfNotExists (CosmosClient cosmosClient, String cosmosDatabase) {
	    CosmosContainerProperties containerProps = new CosmosContainerProperties(containerName, "/id");
//	    ThroughputProperties throughputProps = ThroughputProperties.createManualThroughput(400);
//	    cosmosClient.getDatabase(cosmosDatabase).createContainerIfNotExists(containerProps, throughputProps);
	    cosmosClient.getDatabase(cosmosDatabase).createContainerIfNotExists(containerProps);
	}

	private CosmosContainer getContainer() {
	    return cosmosClient.getDatabase(cosmosDatabase).getContainer(containerName);
	}
	private void set (Session session) {
	    CosmosContainer container = getContainer();
	    container.replaceItem(session,sessionKey,new PartitionKey(sessionKey),new CosmosItemRequestOptions());
	}
	private Session get () {
	    CosmosContainer container = getContainer();
	    Session session = container.readItem(sessionKey,new PartitionKey(sessionKey),Session.class).getItem();
	    return session;
	}
	
	public void create () {
	    Session session = new Session();
	    session.setId(sessionKey);
	    session.setCreatedAt(System.currentTimeMillis());
	    session.setFiles(new File [0]);
	    session.setOwnerKey(null);
	    session.setLocked(false);

	    CosmosContainer container = getContainer();
	    container.createItem(session);
	}
	
	public boolean exists () {
	    List<CosmosItemIdentity> ids = new ArrayList<CosmosItemIdentity>();
	    ids.add(new CosmosItemIdentity(new PartitionKey(sessionKey),sessionKey));
	    
	    CosmosContainer container = getContainer();
	    FeedResponse<Session> res = container.readMany(ids,Session.class);
	    return !res.getResults().isEmpty();
	}
	
	public void setOwnerKey (String ownerKey) {
	    Session session = get ();
	    session.setOwnerKey(ownerKey);
	    set(session);
	}
	
	public boolean matchOwnerKey (String ownerKey) {
	    Session session = get ();
	    return ownerKey.equals(session.getOwnerKey());
	}
	
	public int appendFile (String fileName, String contentType) {
	    Session session = get ();
	    List<File> files = new ArrayList<File>(Arrays.asList(session.getFiles()));
	    File file = new File();
	    file.setFileName(fileName);
	    file.setContentType(contentType);
	    files.add(file);
	    session.setFiles(files.toArray(new File[0]));
	    set(session);
	    return files.size()-1; // [MEMO] return last index for new fileId
	}
	
	public int getFileCount () {
	    Session session = get ();
	    return session.getFiles().length;
	}
	
	public List<FileListItem> getFileList () {
	    Session session = get ();
	    File [] files = session.getFiles();
	    List<FileListItem> result = new ArrayList<FileListItem>();
	    for( File file : files) {
		FileListItem item = new FileListItem(file.getFileName(),file.getContentType());
		result.add(item);
	    }
	    return result;
	}
	
	public boolean isFileNameUsed (String fileName) {
	    boolean result = false;
	    List<FileListItem> files = getFileList();
	    for (FileListItem item : files) {
		if (item.fileName.equals(fileName)) {
		    result = true;
		    break;
		}
	    }
	    return result;
	}
	
	public boolean hasFile (int fileId) {
	    return getFileCount() > fileId;
	}
	
	public FileListItem getFileInfo (int fileId) {
	    Session session = get ();
	    File [] files = session.getFiles();
	    File file = files [fileId];
	    return new FileListItem(file.getFileName(),file.getContentType());
	}
	
	public void lock () {
	    Session session = get ();
	    session.setLocked(true);
	    set(session);
	}
	
	public boolean locked () {
	    Session session = get ();
	    return session.getLocked();
	}
	
	public void delete () {
	    CosmosContainer container = getContainer();
	    container.deleteItem(sessionKey,new PartitionKey(sessionKey),new CosmosItemRequestOptions());
	}
	
	public boolean ziped () {
	    Session session = get ();
	    return session.getZiped();
	}

    }
    
    private static class BlobManager {
	private BlobServiceClient blobServiceClient;
	private String blobServiceContainer;
	private String sessionKey;
	public BlobManager (BlobServiceClient blobServiceClient, String blobServiceContainer, String sessionKey) {
	    this.blobServiceClient = blobServiceClient;
	    this.blobServiceContainer = blobServiceContainer;
	    this.sessionKey = sessionKey;
	}

	private String getFileDataBlobPath(int fileId) {
	    return sessionKey + "/" + Integer.toString(fileId);
	}

	private String getZipFileDataBlobPath() {
	    return sessionKey + "/zip";
	}

	private BlobContainerClient getBlobContainerClient() {
	    return blobServiceClient.getBlobContainerClient(blobServiceContainer);
	}

	public void appendFileData(int fileId) {
	    // create file data file
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient fileDataBlobClient = containerClient.getBlobClient(getFileDataBlobPath(fileId));
	    AppendBlobClient fileDataAppendBlobClient = fileDataBlobClient.getAppendBlobClient();
	    fileDataAppendBlobClient.create();
	}
	public void upload (int fileId, InputStream in, long len) throws IOException {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileDataBlobPath(fileId));
	    AppendBlobClient appendBlobClient = blobClient.getAppendBlobClient();
//	    appendBlobClient.appendBlock(in,len); // unsusable due to maxLimit 4194304 byte error
	    OutputStream out = appendBlobClient.getBlobOutputStream();
	    Util.copy(in,out,1024 * 1024);
	}
	public void download (int fileId, OutputStream out) {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileDataBlobPath(fileId));
	    blobClient.download(out);
	}

	public long getFileSize(int fileId) {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileDataBlobPath(fileId));
	    return blobClient.getProperties().getBlobSize();
	}

	public void deleteAll() {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    for(BlobItem item: containerClient.listBlobsByHierarchy(sessionKey + "/")) {
		BlobClient blobClient = containerClient.getBlobClient(item.getName());
		blobClient.delete();
	    }
	}

	public void zipDownload (OutputStream out) {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getZipFileDataBlobPath());
	    blobClient.download(out);
	}

	public long getZipFileSize() {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getZipFileDataBlobPath());
	    return blobClient.getProperties().getBlobSize();
	}

    }



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
	BlobManager bm = new BlobManager (blobServiceClient,blobServiceContainer,sessionKey);
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
	BlobManager bm = new BlobManager (blobServiceClient,blobServiceContainer,sessionKey);
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
	BlobManager bm = new BlobManager (blobServiceClient,blobServiceContainer,sessionKey);
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
	BlobManager bm = new BlobManager (blobServiceClient,blobServiceContainer,sessionKey);
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
	BlobManager bm = new BlobManager (blobServiceClient,blobServiceContainer,sessionKey);
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
	BlobManager bm = new BlobManager (blobServiceClient,blobServiceContainer,sessionKey);
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
	BlobManager bm = new BlobManager (blobServiceClient,blobServiceContainer,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to zipDownload: invalid session key");
	}
	if(!dm.ziped()) {
	    throw new NoSuchFileDataException("failed to zipDownload: invalid fileId");
	}
	bm.zipDownload (out);
    }

}
