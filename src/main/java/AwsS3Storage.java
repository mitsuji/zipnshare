import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.core.sync.RequestBody;


import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;

import org.mitsuji.vswf.Util;

public class AwsS3Storage implements ZipnshareServlet.DataStorage {

    private static class FileManager {
	private String region;
	private AwsBasicCredentials awsCredentials;
	private String bucketName;
	private String sessionKey;
	public FileManager (String region, AwsBasicCredentials awsCredentials, String bucketName, String sessionKey) {
	    this.region = region;
	    this.awsCredentials = awsCredentials;
	    this.bucketName = bucketName;
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

	private S3Client buildClient() {
	    return S3Client.builder()
		.region(Region.of(region))
		.credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
		.build();
	}

	public void createCreatedatFile() throws IOException {
	    // create _createdat file
	    S3Client client = buildClient();
	    PutObjectRequest req = PutObjectRequest.builder()
		.bucket(bucketName)
		.key(getCreatedatFilePath())
		.build();
	    byte [] bytes = Long.toString(System.currentTimeMillis()).getBytes();
	    client.putObject(req, RequestBody.fromBytes(bytes));
	}
	public void createFileListFile() {
	    // create _files file
	    S3Client client = buildClient();
	    PutObjectRequest req = PutObjectRequest.builder()
		.bucket(bucketName)
		.key(getFileListFilePath())
		.build();
	    byte [] bytes = "".getBytes();
	    client.putObject(req, RequestBody.fromBytes(bytes));
	}
	public void createOwnerKeyFile(String ownerKey) throws IOException {
	    // create ownerKey file
	    S3Client client = buildClient();
	    PutObjectRequest req = PutObjectRequest.builder()
		.bucket(bucketName)
		.key(getOwnerKeyFilePath())
		.build();
	    byte [] bytes = ownerKey.getBytes("UTF-8");
	    client.putObject(req, RequestBody.fromBytes(bytes));
	}
	public int getFileCount() throws IOException {
	    S3Client client = buildClient();
	    GetObjectRequest req = GetObjectRequest.builder()
		.bucket(bucketName)
		.key(getFileListFilePath())
		.build();
	    ResponseInputStream<GetObjectResponse> in = client.getObject(req);
	    if(in.response().contentLength() > 0) {
		int lc = Util.getLineCount(in);
		return lc / 2; // 2 lines for each file
	    } else {
		return 0;
	    }
	}

	private byte [] getFileListBytes () throws IOException {
	    ByteArrayOutputStream out = new ByteArrayOutputStream ();
	    S3Client client = buildClient();
	    GetObjectRequest req = GetObjectRequest.builder()
		.bucket(bucketName)
		.key(getFileListFilePath())
		.build();
	    InputStream in = client.getObject(req);
	    Util.copy(in,out,1024);
	    out.close();
	    return out.toByteArray();
	}
	public String appendFileData(int fileId, String fileName, String contentType) throws IOException {
	    // create file data file
	    S3Client client = buildClient();
	    CreateMultipartUploadRequest multipartReq = CreateMultipartUploadRequest.builder()
		.bucket(bucketName)
		.key(getFileDataFilePath(fileId))
		.build();
	    CreateMultipartUploadResponse multipartRes = client.createMultipartUpload(multipartReq);

	    ByteArrayOutputStream out = new ByteArrayOutputStream ();
	    out.write(getFileListBytes()); // [MEMO] append original
	    out.write(fileName.trim().getBytes("UTF-8"));    out.write('\n');
	    out.write(contentType.trim().getBytes("UTF-8")); out.write('\n');
	    out.close();
	    
	    PutObjectRequest putReq = PutObjectRequest.builder()
		.bucket(bucketName)
		.key(getFileListFilePath())
		.build();
	    client.putObject(putReq, RequestBody.fromBytes(out.toByteArray()));
	    
	    return multipartRes.uploadId();
	}
	public String upload (int fileId, String uploadId, int partNumber, InputStream in, long len) throws IOException {
	    S3Client client = buildClient();
	    UploadPartRequest req = UploadPartRequest.builder()
		.bucket(bucketName)
		.key(getFileDataFilePath(fileId))
		.uploadId(uploadId)
		.partNumber(partNumber+1)
		.build();
	    UploadPartResponse res = client.uploadPart(req, RequestBody.fromInputStream(in,len));
	    return res.eTag();
	}
	public void closeFileData(int fileId, String uploadId, List<String> etags) throws IOException {
	    ArrayList<CompletedPart> parts = new ArrayList();
	    for (int i = 0; i < etags.size(); i++) {
		String etag = etags.get(i);
		CompletedPart part = CompletedPart.builder()
		    .partNumber(i+1)
		    .eTag(etag)
		    .build();
		parts.add(part);
	    }

	    CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
		.parts(parts)
		.build();
	    
	    S3Client client = buildClient();
	    CompleteMultipartUploadRequest req = CompleteMultipartUploadRequest.builder()
		.bucket(bucketName)
		.key(getFileDataFilePath(fileId))
		.uploadId(uploadId)
		.multipartUpload(completedMultipartUpload)
		.build();
	    client.completeMultipartUpload(req);
	}
	public void createLockedFile() throws IOException {
	    // create locked file
	    S3Client client = buildClient();
	    PutObjectRequest req = PutObjectRequest.builder()
		.bucket(bucketName)
		.key(getLockedFilePath())
		.build();
	    byte [] bytes = "".getBytes();
	    client.putObject(req, RequestBody.fromBytes(bytes));
	}
	public List<FileListItem> getFileList() throws IOException {
	    List<FileListItem> result = new ArrayList<FileListItem>();
	    S3Client client = buildClient();
	    GetObjectRequest req = GetObjectRequest.builder()
		.bucket(bucketName)
		.key(getFileListFilePath())
		.build();
	    InputStream in = client.getObject(req);
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
	    S3Client client = buildClient();
	    GetObjectRequest req = GetObjectRequest.builder()
		.bucket(bucketName)
		.key(getFileListFilePath())
		.build();
	    InputStream in = client.getObject(req);
	    BufferedReader reader = new BufferedReader(new InputStreamReader (in, "UTF-8"));
	    for ( int i = 0; i < fileId; i++) {
		reader.readLine(); // [TODO] check null ?
		reader.readLine(); // [TODO] check null ?
	    }
	    String fileName = reader.readLine();
	    String contentType = reader.readLine();
	    return new FileListItem(fileName,contentType);
	}
	public InputStream getFileDataInputStream (int fileId) {
	    S3Client client = buildClient();
	    GetObjectRequest req = GetObjectRequest.builder()
		.bucket(bucketName)
		.key(getFileDataFilePath(fileId))
		.build();
	    return client.getObject(req);
	}

	private static boolean objectExists(S3Client client, HeadObjectRequest req) {
	    try {
		HeadObjectResponse res = client.headObject(req);
		return true;
	    } catch (NoSuchKeyException e) {
		return false;
	    }
	}
	public boolean hasCreatedatFile() {
	    S3Client client = buildClient();
	    HeadObjectRequest req = HeadObjectRequest.builder()
		.bucket(bucketName)
		.key(getCreatedatFilePath())
		.build();
	    return objectExists(client,req);
	}
	public boolean hasLockedFile() {
	    S3Client client = buildClient();
	    HeadObjectRequest req = HeadObjectRequest.builder()
		.bucket(bucketName)
		.key(getLockedFilePath())
		.build();
	    return objectExists(client,req);
	}
	public boolean hasFileDataFile(int fileId) {
	    S3Client client = buildClient();
	    HeadObjectRequest req = HeadObjectRequest.builder()
		.bucket(bucketName)
		.key(getFileDataFilePath(fileId))
		.build();
	    return objectExists(client,req);
	}
	public long getFileSize(int fileId) throws IOException {
	    S3Client client = buildClient();
	    HeadObjectRequest req = HeadObjectRequest.builder()
		.bucket(bucketName)
		.key(getFileDataFilePath(fileId))
		.build();
	    HeadObjectResponse res = client.headObject(req);
	    return res.contentLength();
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
	    S3Client client = buildClient();
	    GetObjectRequest req = GetObjectRequest.builder()
		.bucket(bucketName)
		.key(getOwnerKeyFilePath())
		.build();
	    InputStream in = client.getObject(req);
	    Util.copy(in,out,1024);
	    out.close();
	    String fileOwnerKey = new String(out.toByteArray(),"UTF-8");
	    return ownerKey.equals(fileOwnerKey);
	}
	public void deleteSession() throws IOException {
	    S3Client client = buildClient();
	    ListObjectsRequest listReq = ListObjectsRequest.builder()
		.bucket(bucketName)
		.prefix(sessionKey + "/")
		.build();
	    ListObjectsResponse listRes = client.listObjects(listReq);
	    for(S3Object obj : listRes.contents()) {
		DeleteObjectRequest req = DeleteObjectRequest.builder()
		    .bucket(bucketName)
		    .key(obj.key())
		    .build();
		client.deleteObject(req);
	    }
	    
	}

    }


    // {sessionKey:{fileId:UploadId}}
    private HashMap<String,HashMap<String,String>> sessionKeyTofileIdToUploadIdMap;
    
    // {uploadId:[Etag]}
    private HashMap<String,ArrayList<String>> uploadIdToEtagsMap;
    
    // {uploadId:filSize}
    private HashMap<String,Long> uploadIdToFileSizeMap;

    private String region;
    private AwsBasicCredentials awsCredentials;
    private String bucketName;
    private int maxFileCount;
    public AwsS3Storage (String region, String accessKeyId, String secretAccessKey, String bucketName, int maxFileCount) throws IOException {
	sessionKeyTofileIdToUploadIdMap = new HashMap();
	uploadIdToEtagsMap = new HashMap();
	uploadIdToFileSizeMap = new HashMap();
	this.region = region;
	awsCredentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
	this.bucketName = bucketName;
	this.maxFileCount = maxFileCount;
    }

    public String createSession () throws DataStorageException {
	String sessionKey = Util.genAlphaNumericKey(16);
	try {
	    FileManager fm = new FileManager (region,awsCredentials,bucketName,sessionKey);
	    fm.createCreatedatFile();
	    fm.createFileListFile();
	} catch (IOException  ex) {
	    throw new DataStorageException("failed to createSession", ex);
	}
	return sessionKey;
    }
    public void setOwnerKey (String sessionKey, String ownerKey) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (region,awsCredentials,bucketName,sessionKey);
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
	    FileManager fm = new FileManager (region,awsCredentials,bucketName,sessionKey);
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
	    String uploadId = fm.appendFileData(fileCount,fileName,contentType);
	    String fileId = Integer.toString(fileCount);
	    synchronized (this) {
		HashMap<String,String> fileIdToUploadIdMap;
		if (sessionKeyTofileIdToUploadIdMap.containsKey(sessionKey)){
		    fileIdToUploadIdMap = sessionKeyTofileIdToUploadIdMap.get(sessionKey);
		} else {
		    fileIdToUploadIdMap = new HashMap();
		    sessionKeyTofileIdToUploadIdMap.put(sessionKey, fileIdToUploadIdMap);
		}
		fileIdToUploadIdMap.put(fileId,uploadId);
		uploadIdToEtagsMap.put(uploadId, new ArrayList());
		uploadIdToFileSizeMap.put(uploadId, 0L);
	    }
	    return fileId;
	} catch (IOException ex) {
	    throw new DataStorageException("failed to createFileData", ex);
	}
    }
    public void upload (String sessionKey, String fileId, InputStream in, long len) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (region,awsCredentials,bucketName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to upload: invalid session key");
	    }
	    // [MEMO] imcomplete multipart
	    if(fm.hasFileDataFile(Integer.valueOf(fileId))) {
		throw new NoSuchFileDataException("failed to upload: invalid fileId");
	    }
	    if (!sessionKeyTofileIdToUploadIdMap.containsKey(sessionKey)){
		throw new DataStorageException("failed to upload: invalid session key 2");
	    }
	    HashMap<String,String> fileIdToUploadIdMap = sessionKeyTofileIdToUploadIdMap.get(sessionKey);
	    if (!fileIdToUploadIdMap.containsKey(fileId)){
		throw new DataStorageException("failed to upload: invalid fileId");
	    }
	    String uploadId = fileIdToUploadIdMap.get(fileId);
	    if ((!uploadIdToEtagsMap.containsKey(uploadId)) || (!uploadIdToFileSizeMap.containsKey(uploadId))) {
		throw new DataStorageException("failed to upload: invalid uploadId");
	    }
	    List<String> etags = uploadIdToEtagsMap.get(uploadId);
	    String etag = fm.upload(Integer.valueOf(fileId),uploadId,etags.size(),in,len);
	    synchronized (this) {
		etags.add(etag);
		long fileSize = uploadIdToFileSizeMap.get(uploadId) + len;
		uploadIdToFileSizeMap.put(uploadId,fileSize);
	    }
	} catch (IOException ex) {
	    throw new DataStorageException("failed to upload",ex);
	}
    }
    public void closeFileData (String sessionKey, String fileId) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (region,awsCredentials,bucketName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to closeFileData: invalid session key");
	    }
	    // [MEMO] imcomplete multipart
	    if(fm.hasFileDataFile(Integer.valueOf(fileId))) {
		throw new NoSuchFileDataException("failed to closeFileData: invalid fileId");
	    }
	    if (!sessionKeyTofileIdToUploadIdMap.containsKey(sessionKey)){
		throw new DataStorageException("failed to closeFileData: invalid session key 2");
	    }
	    HashMap<String,String> fileIdToUploadIdMap = sessionKeyTofileIdToUploadIdMap.get(sessionKey);
	    if (!fileIdToUploadIdMap.containsKey(fileId)){
		throw new DataStorageException("failed to closeFileData: invalid fileId");
	    }
	    String uploadId = fileIdToUploadIdMap.get(fileId);
	    if (!uploadIdToEtagsMap.containsKey(uploadId)) {
		throw new DataStorageException("failed to closeFileData: invalid uploadId");
	    }
	    List<String> etags = uploadIdToEtagsMap.get(uploadId);
	    fm.closeFileData(Integer.valueOf(fileId),uploadId,etags);
	    synchronized (this) {
		fileIdToUploadIdMap.remove(fileId);
		uploadIdToEtagsMap.remove(uploadId);
		uploadIdToFileSizeMap.remove(uploadId);

	    }
	} catch (IOException ex) {
	    throw new DataStorageException("failed to upload",ex);
	}
    }
    public void lockSession (String sessionKey) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (region,awsCredentials,bucketName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to lockSession: invalid session key");
	    }
	    if(fm.hasLockedFile()) {
		throw new DataStorageException("failed to lockSession: session already locked");
	    }
	    fm.createLockedFile();
	    synchronized (this) {
		sessionKeyTofileIdToUploadIdMap.remove(sessionKey);
	    }
	} catch (IOException ex) {
	    throw new DataStorageException("failed to lockSession",ex);
	}
    }

    public boolean hasLocked (String sessionKey) throws DataStorageException {
	FileManager fm = new FileManager (region,awsCredentials,bucketName,sessionKey);
	if(!fm.hasCreatedatFile()) {
	    throw new NoSuchSessionException("failed to hasLocked: invalid session key");
	}
	return fm.hasLockedFile();
    }
    public long getFileSize (String sessionKey, String fileId) throws DataStorageException {
	// [MEMO] size of incomplete multipart
	if (sessionKeyTofileIdToUploadIdMap.containsKey(sessionKey)){
	    HashMap<String,String> fileIdToUploadIdMap = sessionKeyTofileIdToUploadIdMap.get(sessionKey);
	    if (fileIdToUploadIdMap.containsKey(fileId)){
		String uploadId = fileIdToUploadIdMap.get(fileId);
		if (uploadIdToFileSizeMap.containsKey(uploadId)) {
		    return uploadIdToFileSizeMap.get(uploadId);
		} else {
		    throw new DataStorageException("failed to getFileSize: invalid uploadId");
		}
	    }
	}

	try {
	    FileManager fm = new FileManager (region,awsCredentials,bucketName,sessionKey);
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
	    FileManager fm = new FileManager (region,awsCredentials,bucketName,sessionKey);
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
	    FileManager fm = new FileManager (region,awsCredentials,bucketName,sessionKey);
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
	try {
	    FileManager fm = new FileManager (region,awsCredentials,bucketName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to download: invalid session key");
	    }
	    if(!fm.hasFileDataFile(Integer.valueOf(fileId))) {
		throw new NoSuchFileDataException("failed to download: invalid fileId");
	    }
	    InputStream in = fm.getFileDataInputStream (Integer.valueOf(fileId));
	    Util.copy(in,out,1024 * 1024);
	} catch (IOException ex) {
	    throw new DataStorageException("failed to download",ex);
	}
    }

    public boolean matchOwnerKey (String sessionKey, String ownerKey) throws DataStorageException {
	try {
	    FileManager fm = new FileManager (region,awsCredentials,bucketName,sessionKey);
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
	    FileManager fm = new FileManager (region,awsCredentials,bucketName,sessionKey);
	    if(!fm.hasCreatedatFile()) {
		throw new NoSuchSessionException("failed to deleteSession: invalid session key");
	    }
	    fm.deleteSession();
	} catch (IOException ex) {
	    throw new DataStorageException("failed to deleteSession",ex);
	}

    }

}
