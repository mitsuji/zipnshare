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

import com.azure.storage.blob.*;
import com.azure.storage.blob.specialized.*;
import com.azure.storage.blob.models.*;

import org.mitsuji.vswf.Util;

public class AzureBlobStorageV12 implements ZipnshareServlet.DataStorage {

    private static class FileManager {
	private BlobServiceClientBuilder clientBuilder;
	private String containerName;
	private String sessionKey;
	public FileManager (BlobServiceClientBuilder clientBuilder, String containerName, String sessionKey) {
	    this.clientBuilder = clientBuilder;
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

	private BlobContainerClient getBlobContainerClient() {
	    BlobServiceClient blobServiceClient = clientBuilder.buildClient();
	    return blobServiceClient.getBlobContainerClient(containerName);
	}

	public void createCreatedatFile() throws IOException {
	    // create _createdat file
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getCreatedatFilePath());
	    BlockBlobClient blockBlobClient = blobClient.getBlockBlobClient();
	    OutputStream out = blockBlobClient.getBlobOutputStream();
	    out.write(Long.toString(System.currentTimeMillis()).getBytes());
	    out.close();
	}
	public void createFileListFile() {
	    // create _files file
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileListFilePath());
	    AppendBlobClient appendBlobClient = blobClient.getAppendBlobClient();
	    appendBlobClient.create();
	}
	public void createOwnerKeyFile(String ownerKey) throws IOException {
	    // create ownerKey file
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getOwnerKeyFilePath());
	    BlockBlobClient blockBlobClient = blobClient.getBlockBlobClient();
	    OutputStream out = blockBlobClient.getBlobOutputStream();
	    out.write(ownerKey.getBytes("UTF-8"));
	    out.close();
	}
	public int getFileCount() throws IOException {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileListFilePath());
	    InputStream in = blobClient.openInputStream();
	    int lc = Util.getLineCount(in);
	    return lc / 2; // 2 lines for each file
	}
	public void appendFileData(int fileId, String fileName, String contentType) throws IOException {
	    // create file data file
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient fileDataBlobClient = containerClient.getBlobClient(getFileDataFilePath(fileId));
	    AppendBlobClient fileDataAppendBlobClient = fileDataBlobClient.getAppendBlobClient();
	    fileDataAppendBlobClient.create();
	    BlobClient fileListBlobClient = containerClient.getBlobClient(getFileListFilePath());
	    AppendBlobClient fileListAppendBlobClient = fileListBlobClient.getAppendBlobClient();
	    OutputStream out = fileListAppendBlobClient.getBlobOutputStream();
	    out.write(fileName.trim().getBytes("UTF-8"));    out.write('\n');
	    out.write(contentType.trim().getBytes("UTF-8")); out.write('\n');
	    out.close();
	}
	public void upload (int fileId, InputStream in, long len) throws IOException {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileDataFilePath(fileId));
	    AppendBlobClient appendBlobClient = blobClient.getAppendBlobClient();
	    appendBlobClient.appendBlock(in,len);
	}
	public void createLockedFile() throws IOException {
	    // create locked file
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getLockedFilePath());
	    BlockBlobClient blockBlobClient = blobClient.getBlockBlobClient();
	    OutputStream out = blockBlobClient.getBlobOutputStream();
	    out.close(); // flush empty file
	}
	public List<FileListItem> getFileList() throws IOException {
	    List<FileListItem> result = new ArrayList<FileListItem>();
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileListFilePath());
	    InputStream in = blobClient.openInputStream();
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
	public FileListItem getFileInfo(int fileId) throws IOException {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileListFilePath());
	    InputStream in = blobClient.openInputStream();
	    BufferedReader reader = new BufferedReader(new InputStreamReader (in, "UTF-8"));
	    for ( int i = 0; i < fileId; i++) {
		reader.readLine(); // [TODO] check null ?
		reader.readLine(); // [TODO] check null ?
	    }
	    String fileName = reader.readLine();
	    String contentType = reader.readLine();
	    return new FileListItem(fileName,contentType);
	}
	public void download (int fileId, OutputStream out) {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileDataFilePath(fileId));
	    blobClient.download(out);
	}

	public boolean hasCreatedatFile() {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getCreatedatFilePath());
	    return blobClient.exists();
	}
	public boolean hasLockedFile() {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getLockedFilePath());
	    return blobClient.exists();
	}
	public boolean hasFileDataFile(int fileId) {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileDataFilePath(fileId));
	    return blobClient.exists();
	}
	public long getFileSize(int fileId) throws IOException {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileDataFilePath(fileId));
	    return blobClient.getProperties().getBlobSize();
	}
	public boolean isFileNameUsed(String fileName) throws IOException {
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
	public boolean matchOwnerKey(String ownerKey) throws IOException {
	    ByteArrayOutputStream out = new ByteArrayOutputStream ();
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getOwnerKeyFilePath());
	    InputStream in = blobClient.openInputStream();
	    Util.copy(in,out,1024);
	    out.close();
	    String fileOwnerKey = new String(out.toByteArray(),"UTF-8");
	    return ownerKey.equals(fileOwnerKey);
	}
	public void deleteSession() throws IOException {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    for(BlobItem item: containerClient.listBlobsByHierarchy(sessionKey + "/")) {
		BlobClient blobClient = containerClient.getBlobClient(item.getName());
		blobClient.delete();
	    }
	}

    }

    private BlobServiceClientBuilder storageAccount;
    private String containerName;
    private int maxFileCount;
    private long maxFileSize;
    public AzureBlobStorageV12 (String azureBlobCS, String containerName, int maxFileCount, long maxFileSize) throws IOException {

	storageAccount = new BlobServiceClientBuilder().connectionString(azureBlobCS);
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
	} catch (IOException  ex) {
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
	} catch (IOException  ex) {
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
	} catch (IOException ex) {
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
	} catch (IOException ex) {
	    throw new DataStorageException("failed to upload",ex);
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
	} catch (IOException ex) {
	    throw new DataStorageException("failed to lockSession",ex);
	}
    }

    public boolean hasLocked (String sessionKey) throws DataStorageException {
	FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	if(!fm.hasCreatedatFile()) {
	    throw new NoSuchSessionException("failed to hasLocked: invalid session key");
	}
	return fm.hasLockedFile();
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
	} catch (IOException ex) {
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
	} catch (IOException ex) {
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
	} catch (IOException ex) {
	    throw new DataStorageException("failed to getFileInfo",ex);
	}
    }

    public void download (String sessionKey, String fileId, OutputStream out) throws DataStorageException {
	FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	if(!fm.hasCreatedatFile()) {
	    throw new NoSuchSessionException("failed to download: invalid session key");
	}
	if(!fm.hasFileDataFile(Integer.valueOf(fileId))) {
	    throw new NoSuchFileDataException("failed to download: invalid fileId");
	}
	fm.download (Integer.valueOf(fileId),out);
    }

    public boolean matchOwnerKey (String sessionKey, String ownerKey) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (storageAccount,containerName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to matchOwnerKey: invalid session key");
	    }
	    return fm.matchOwnerKey(ownerKey);
	} catch (IOException ex) {
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
	} catch (IOException ex) {
	    throw new DataStorageException("failed to deleteSession",ex);
	}

    }

}
