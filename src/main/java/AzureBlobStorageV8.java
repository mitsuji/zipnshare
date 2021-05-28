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

import org.mitsuji.vswf.Util;

public class AzureBlobStorageV8 implements ZipnshareServlet.DataStorage {
    
    private static class FileManager {
	private CloudStorageAccount storageAccount;
	private String containerName;
	private String sessionKey;
	public FileManager (CloudStorageAccount storageAccount, String containerName, String sessionKey) {
	    this.storageAccount = storageAccount;
	    this.containerName = containerName;
	    this.sessionKey = sessionKey;
	}

	private String getFileListFilePath() {
	    return sessionKey + "/_files";
	}
	private String getCreatedatFilePath() {
	    return sessionKey + "/_createdat";
	}
	private String getOwnerKeyFilePath() {
	    return sessionKey + "/ownerKey";
	}
	private String getFileDataFilePath(int fileId) {
	    return sessionKey + "/" + Integer.toString(fileId);
	}
	private String getLockedFilePath() {
	    return sessionKey + "/_locked";
	}

	private CloudBlobContainer getBlobContainer() throws URISyntaxException, StorageException {
	    CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
	    return blobClient.getContainerReference(containerName);
	}

	public void createCreatedatFile() throws URISyntaxException, StorageException, IOException {
	    // create _createdat file
	    CloudBlobContainer container = getBlobContainer();
	    CloudBlockBlob blob = container.getBlockBlobReference(getCreatedatFilePath());
	    blob.uploadText(Long.toString(System.currentTimeMillis())); // unix epoch
	}
	public void createFileListFile() throws URISyntaxException, StorageException, IOException {
	    // create _files file
	    CloudBlobContainer container = getBlobContainer();
	    CloudAppendBlob blob = container.getAppendBlobReference(getFileListFilePath());
	    blob.createOrReplace();
	}
	public void createOwnerKeyFile(String ownerKey) throws URISyntaxException, StorageException, IOException {
	    // create ownerKey file
	    CloudBlobContainer container = getBlobContainer();
	    CloudBlockBlob blob = container.getBlockBlobReference(getOwnerKeyFilePath());
	    OutputStream out = blob.openOutputStream();
	    out.write(ownerKey.getBytes("UTF-8"));
	    out.close();
	}
	public int getFileCount() throws URISyntaxException, StorageException, IOException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudAppendBlob blob = container.getAppendBlobReference(getFileListFilePath());
	    InputStream in = blob.openInputStream();
	    int lc = Util.getLineCount(in);
	    return lc / 2; // 2 lines for each file
	}
	public void appendFileData(int fileId, String fileName, String contentType) throws URISyntaxException, StorageException, IOException {
	    // create file data file
	    CloudBlobContainer container = getBlobContainer();
	    CloudAppendBlob fileDataBlob = container.getAppendBlobReference(getFileDataFilePath(fileId));
	    fileDataBlob.createOrReplace();
	    CloudAppendBlob fileListBlob = container.getAppendBlobReference(getFileListFilePath());
	    OutputStream out = fileListBlob.openWriteExisting();
	    out.write(fileName.trim().getBytes("UTF-8"));    out.write('\n');
	    out.write(contentType.trim().getBytes("UTF-8")); out.write('\n');
	    out.close();
	}
	public void upload (int fileId, InputStream in, long len) throws URISyntaxException, StorageException, IOException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudAppendBlob fileDataBlob = container.getAppendBlobReference(getFileDataFilePath(fileId));
	    fileDataBlob.append(in,len);
	}
	public void createLockedFile() throws URISyntaxException, StorageException, IOException {
	    // create locked file
	    CloudBlobContainer container = getBlobContainer();
	    CloudBlockBlob blob = container.getBlockBlobReference(getLockedFilePath());
	    OutputStream out = blob.openOutputStream();
	    out.close(); // flush empty file
	}
	public List<FileListItem> getFileList() throws URISyntaxException, StorageException, IOException {
	    List<FileListItem> result = new ArrayList<FileListItem>();
	    CloudBlobContainer container = getBlobContainer();
	    CloudAppendBlob blob = container.getAppendBlobReference(getFileListFilePath());
	    InputStream in = blob.openInputStream();
	    BufferedReader reader = new BufferedReader(new InputStreamReader (in, "UTF-8"));
	    int i = 0;
	    String line1;
	    String line2;
	    while ((line1 = reader.readLine()) != null ) {
		line2 = reader.readLine(); // [MEMO] 2 lines for each file
		FileListItem item = new FileListItem(line1,line2);
		result.add(item);
		i++;
	    }
	    return result;
	}
	public FileListItem getFileInfo(int fileId) throws URISyntaxException, StorageException, IOException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudAppendBlob blob = container.getAppendBlobReference(getFileListFilePath());
	    InputStream in = blob.openInputStream();
	    BufferedReader reader = new BufferedReader(new InputStreamReader (in, "UTF-8"));
	    for ( int i = 0; i < fileId; i++) {
		reader.readLine(); // [TODO] check null ?
		reader.readLine(); // [TODO] check null ?
	    }
	    String fileName = reader.readLine();
	    String contentType = reader.readLine();
	    return new FileListItem(fileName,contentType);
	}
	public InputStream getFileDataInputStream (int fileId) throws URISyntaxException, StorageException, IOException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudAppendBlob blob = container.getAppendBlobReference(getFileDataFilePath(fileId));
	    return blob.openInputStream();
	}

	public boolean hasCreatedatFile() throws URISyntaxException, StorageException, IOException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudBlob blob = container.getBlockBlobReference(getCreatedatFilePath());
	    return blob.exists();
	}
	public boolean hasLockedFile() throws URISyntaxException, StorageException, IOException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudBlob blob = container.getBlockBlobReference(getLockedFilePath());
	    return blob.exists();
	}
	public boolean hasFileDataFile(int fileId) throws URISyntaxException, StorageException, IOException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudBlob blob = container.getAppendBlobReference(getFileDataFilePath(fileId));
	    return blob.exists();
	}
	public long getFileSize(int fileId) throws URISyntaxException, StorageException, IOException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudBlob blob = container.getAppendBlobReference(getFileDataFilePath(fileId));
	    blob.downloadAttributes();
	    return blob.getProperties().getLength();
	}
	public boolean isFileNameUsed(String fileName) throws URISyntaxException, StorageException, IOException {
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
	public boolean matchOwnerKey(String ownerKey) throws  URISyntaxException, StorageException, IOException {
	    ByteArrayOutputStream out = new ByteArrayOutputStream ();
	    CloudBlobContainer container = getBlobContainer();
	    CloudBlob blob = container.getBlockBlobReference(getOwnerKeyFilePath());
	    InputStream in = blob.openInputStream();
	    Util.copy(in,out,1024);
	    out.close();
	    String fileOwnerKey = new String(out.toByteArray(),"UTF-8");
	    return ownerKey.equals(fileOwnerKey);
	}
	public void deleteSession() throws  URISyntaxException, StorageException, IOException {
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

    private CloudStorageAccount storageAccount;
    private String containerName;
    private int maxFileCount;
    private long maxFileSize;
    public AzureBlobStorageV8 (String azureBlobCS, String containerName, int maxFileCount, long maxFileSize) throws IOException {
	try {
	    storageAccount = CloudStorageAccount.parse(azureBlobCS);
	} catch (URISyntaxException | InvalidKeyException  ex) {
	    throw new IOException("failed to create storageAccount", ex);
	}
	this.containerName = containerName;
	this.maxFileCount = maxFileCount;
	this.maxFileSize = maxFileSize;
    }

    public String createSession () throws DataStorageException {
	String sessionKey = Util.genAlphaNumericKey(16);
	try {
	    FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	    fm.createCreatedatFile();
	    fm.createFileListFile();
	} catch (URISyntaxException | StorageException | IOException  ex) {
	    throw new DataStorageException("failed to createSession", ex);
	}
	return sessionKey;
    }
    public void setOwnerKey (String sessionKey, String ownerKey) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to setOwnerKey: invalid session key");
	    }
	    fm.createOwnerKeyFile(ownerKey);
	} catch (URISyntaxException | StorageException | IOException  ex) {
	    throw new DataStorageException("failed to setOwnerKey", ex);
	}
    }
    public String createFileData (String sessionKey, String fileName, String contentType) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to crateFileData: invalid session key");
	    }
	    // check file count limit
	    int fileCount = fm.getFileCount();
	    if (fileCount >= maxFileCount) {
		throw new TooManyFilesException("fiailed to crateFileData: too many files");
	    }
	    // check fileName duplication
	    if (fm.isFileNameUsed(fileName)) {
		throw new DuplicatedFileNameException("fiailed to crateFileData: duplicated file name");
	    }
	    fm.appendFileData(fileCount,fileName,contentType);
	    return Integer.toString(fileCount);
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to createFileData", ex);
	}
    }
    public void upload (String sessionKey, String fileId, InputStream in, long len) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to upload: invalid session key");
	    }
	    if(!fm.hasFileDataFile(Integer.valueOf(fileId))) {
		throw new NoSuchFileDataException("failed to upload: invalid fileId");
	    }
	    long fileSizeBefore = fm.getFileSize(Integer.valueOf(fileId));
	    long fileSizeAfter = fileSizeBefore + len;
	    if (fileSizeAfter > maxFileSize) {
		throw new TooLargeFileException("failed to upload: too large file");
	    }
	    fm.upload(Integer.valueOf(fileId),in,len);
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to upload", ex);
	}
    }
    public void closeFileData (String sessionKey, String fileId) throws DataStorageException {
	// do nothing
    }
    public void lockSession (String sessionKey) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to lockSession: invalid session key");
	    }
	    if(fm.hasLockedFile()) {
		throw new DataStorageException("failed to lockSession: session already locked");
	    }
	    fm.createLockedFile();
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to lockSession",ex);
	}
    }

    public boolean hasLocked (String sessionKey) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to hasLocked: invalid session key");
	    }
	    return fm.hasLockedFile();
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to hasLocked",ex);
	}
    }
    public long getFileSize (String sessionKey, String fileId) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to getFileSize: invalid session key");
	    }
	    if(!fm.hasFileDataFile(Integer.valueOf(fileId))) {
		throw new NoSuchFileDataException("failed to getFileSize: invalid fileId");
	    }
	    return fm.getFileSize(Integer.valueOf(fileId));
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to getFileSize",ex);
	}
    }
    public List<FileListItem> getFileList(String sessionKey) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to getFileList: invalid session key");
	    }
	    return fm.getFileList();
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to getFileList",ex);
	}
    }
    public FileListItem getFileInfo(String sessionKey, String fileId) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to getFileInfo: invalid session key");
	    }
	    if(!fm.hasFileDataFile(Integer.valueOf(fileId))) {
		throw new NoSuchFileDataException("failed to getFileInfo: invalid fileId");
	    }
	    return fm.getFileInfo(Integer.valueOf(fileId));
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to getFileInfo",ex);
	}
    }

    public void download (String sessionKey, String fileId, OutputStream out) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to download: invalid session key");
	    }
	    if(!fm.hasFileDataFile(Integer.valueOf(fileId))) {
		throw new NoSuchFileDataException("failed to download: invalid fileId");
	    }
	    InputStream in = fm.getFileDataInputStream (Integer.valueOf(fileId));
	    Util.copy(in,out,1024 * 1024);
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to download",ex);
	}
    }

    public boolean matchOwnerKey (String sessionKey, String ownerKey) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to matchOwnerKey: invalid session key");
	    }
	    return fm.matchOwnerKey(ownerKey);
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to matchOwnerKey",ex);
	}
    }

    public void deleteSession (String sessionKey) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to deleteSession: invalid session key");
	    }
	    fm.deleteSession();
	} catch (URISyntaxException | StorageException | IOException ex) {
	    throw new DataStorageException("failed to deleteSession",ex);
	}

    }

}
