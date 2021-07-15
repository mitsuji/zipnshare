package aws;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
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

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import org.mitsuji.vswf.Util;
import type.FileListItem;
import type.DataStorage;

public class AwsS3Storage implements DataStorage {

    private int keyLength;
    private DynamoDbClient dynamoDbClient;
    private String dynamoTable;
    private S3Client s3Client;
    private String s3Bucket;
    private SqsClient sqsClient;
    private String sqsUrl;
    private String sqsGroupId;
    private int maxFileCount;
    private long maxFileSize;
    private boolean useZipConverter;
    public AwsS3Storage (int keyLength, String region, String accessKeyId, String secretAccessKey, String dynamoTable, String s3Bucket, String sqsUrl, String sqsGroupId, int maxFileCount, long maxFileSize, boolean useZipConverter) {
	this.keyLength = keyLength;
	AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
	dynamoDbClient = DynamoDbClient.builder()
	    .region(Region.of(region))
	    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
	    .build();
	this.dynamoTable = dynamoTable;
	s3Client = S3Client.builder()
	    .region(Region.of(region))
	    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
	    .build();
	this.s3Bucket = s3Bucket;
	if (useZipConverter) {
	    sqsClient = SqsClient.builder()
		.region(Region.of(region))
		.credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
		.build();
	    this.sqsUrl = sqsUrl;
	    this.sqsGroupId = sqsGroupId;
	}
	this.maxFileCount = maxFileCount;
	this.maxFileSize = maxFileSize;
	this.useZipConverter = useZipConverter;
    }

    public void init () {
	if(!DatabaseManager.tableExists(dynamoDbClient,dynamoTable)) {
	    DatabaseManager.createTable(dynamoDbClient,dynamoTable);
	}
    }

    public void destroy () {
	dynamoDbClient.close();
	s3Client.close();
    }

    public String createSession () throws DataStorageException {
	String sessionKey = Util.genAlphaNumericKey(keyLength);
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	dm.create();
	return sessionKey;
    }
    public void setOwnerKey (String sessionKey, String ownerKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to setOwnerKey: invalid session key");
	}
	dm.setOwnerKey(ownerKey);
    }
    public String createFileData (String sessionKey, String fileName, String contentType) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	BlobManager bm = new BlobManager (s3Client,s3Bucket,sessionKey);
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
	String uploadId = bm.createMultiPart(fileId);
	dm.putUploadIdForFileId(fileId,uploadId);
	return Integer.toString(fileId);
    }
    public void upload (String sessionKey, String fileId, InputStream in, long len) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	BlobManager bm = new BlobManager (s3Client,s3Bucket,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to upload: invalid session key");
	}
	if(!dm.hasFile(Integer.valueOf(fileId))) {
	    throw new NoSuchFileDataException("failed to upload: invalid fileId");
	}

	String uploadId = dm.getUploadIdForFileId(Integer.valueOf(fileId));
	List<String> etags = dm.getEtagsForFileId(Integer.valueOf(fileId));
	if (etags == null) {
	    etags = new ArrayList<String>();
	}
	Long oFileSizeBefore = dm.getFileSizeForFileId(Integer.valueOf(fileId));
	long fileSizeBefore;
	if (oFileSizeBefore == null ) {
	    fileSizeBefore = 0;
	} else {
	    fileSizeBefore = oFileSizeBefore;
	}
	long fileSizeAfter = fileSizeBefore + len;
	if (fileSizeAfter > maxFileSize) {
	    throw new TooLargeFileException("failed to upload: too large file");
	}
	String etag = bm.uploadPart(Integer.valueOf(fileId),uploadId,etags.size(),in,len);
	dm.addEtagForUploadId(uploadId,etag);
	dm.putFileSizeForUploadId(uploadId,fileSizeAfter);
    }
    public void closeFileData (String sessionKey, String fileId) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	BlobManager bm = new BlobManager (s3Client,s3Bucket,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to closeFileData: invalid session key");
	}
	if(!dm.hasFile(Integer.valueOf(fileId))) {
	    throw new NoSuchFileDataException("failed to closeFileData: invalid fileId");
	}

	String uploadId = dm.getUploadIdForFileId(Integer.valueOf(fileId));
	List<String> etags = dm.getEtagsForFileId(Integer.valueOf(fileId));
	bm.completeMultiPart(Integer.valueOf(fileId),uploadId,etags);
    }
    public void lockSession (String sessionKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to lockSession: invalid session key");
	}
	if(dm.locked()) {
	    throw new DataStorageException("failed to lockSession: session already locked");
	}
	dm.lock();
	if (useZipConverter ) {
	    SendMessageRequest req = SendMessageRequest.builder()
		.queueUrl(sqsUrl)
		.messageGroupId(sqsGroupId)
		.messageBody(sessionKey)
		.build();
	    sqsClient.sendMessage(req);
	}
    }

    public boolean hasLocked (String sessionKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to hasLocked: invalid session key");
	}
	return dm.locked();
    }
    public long getFileSize (String sessionKey, String fileId) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	BlobManager bm = new BlobManager (s3Client,s3Bucket,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to getFileSize: invalid session key");
	}
	if(!dm.hasFile(Integer.valueOf(fileId))) {
	    throw new NoSuchFileDataException("failed to getFileSize: invalid fileId");
	}
	return bm.getFileSize(Integer.valueOf(fileId));
    }
    public List<FileListItem> getFileList(String sessionKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to getFileList: invalid session key");
	}
	return dm.getFileList();
    }
    public FileListItem getFileInfo(String sessionKey, String fileId) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to getFileInfo: invalid session key");
	}
	if(!dm.hasFile(Integer.valueOf(fileId))) {
	    throw new NoSuchFileDataException("failed to getFileInfo: invalid fileId");
	}
	return dm.getFileInfo(Integer.valueOf(fileId));
    }

    public void download (String sessionKey, String fileId, OutputStream out) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	BlobManager bm = new BlobManager (s3Client,s3Bucket,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to download: invalid session key");
	}
	if(!dm.hasFile(Integer.valueOf(fileId))) {
	    throw new NoSuchFileDataException("failed to download: invalid fileId");
	}
	InputStream in = bm.getFileDataInputStream (Integer.valueOf(fileId));
	try {
	    Util.copy(in,out,20 * 1024 * 1024);
	} catch (IOException ex) {
	    throw new DataStorageException("failed to download",ex);
	}
    }

    public boolean matchOwnerKey (String sessionKey, String ownerKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to matchOwnerKey: invalid session key");
	}
	return dm.matchOwnerKey(ownerKey);
    }

    public void deleteSession (String sessionKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	BlobManager bm = new BlobManager (s3Client,s3Bucket,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to deleteSession: invalid session key");
	}
	bm.deleteAll();
	dm.delete();
    }

    public boolean hasZiped (String sessionKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to hasZiped: invalid session key");
	}
	return dm.ziped();
    }
    public long getZipFileSize (String sessionKey) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	BlobManager bm = new BlobManager (s3Client,s3Bucket,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to getZipFileSize: invalid session key");
	}
	if(!dm.ziped()) {
	    throw new NoSuchFileDataException("failed to getZipFileSize: invalid fileId");
	}
	return bm.getZipFileSize();
    }
    public void zipDownload (String sessionKey, OutputStream out) throws DataStorageException {
	DatabaseManager dm = new DatabaseManager (dynamoDbClient,dynamoTable,sessionKey);
	BlobManager bm = new BlobManager (s3Client,s3Bucket,sessionKey);
	if(!dm.exists()) {
	    throw new NoSuchSessionException("failed to zipDownload: invalid session key");
	}
	if(!dm.ziped()) {
	    throw new NoSuchFileDataException("failed to zipDownload: invalid fileId");
	}
	InputStream in = bm.getZipFileDataInputStream ();
	try {
	    Util.copy(in,out,20 * 1024 * 1024);
	} catch (IOException ex) {
	    throw new DataStorageException("failed to zipDownload",ex);
	}
    }

}
