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

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

import org.mitsuji.vswf.Util;

public class AwsS3Storage implements ZipnshareServlet.DataStorage {

    private static class DatabaseManager {

	private DynamoDbClient dynamoDbClient;
	private String tableName;
	private String sessionKey;
	public DatabaseManager (DynamoDbClient dynamoDbClient, String tableName, String sessionKey) {
	    this.dynamoDbClient = dynamoDbClient;
	    this.tableName = tableName;
	    this.sessionKey = sessionKey;
	}

	public static void createTable (DynamoDbClient dynamoDbClient, String tableName) {
	    CreateTableRequest req = CreateTableRequest.builder()
		.attributeDefinitions(
				      AttributeDefinition.builder()
				      .attributeName("sessionKey")
				      .attributeType(ScalarAttributeType.S)
				      .build())
		.keySchema(
			   KeySchemaElement.builder()
			   .attributeName("sessionKey")
			   .keyType(KeyType.HASH)
			   .build())
		.provisionedThroughput(
				       ProvisionedThroughput.builder()
				       .readCapacityUnits(new Long(1))
				       .writeCapacityUnits(new Long(1)).build())
		.tableName(tableName)
		.build();

	    dynamoDbClient.createTable(req);
	}

	public static boolean tableExists(DynamoDbClient dynamoDbClient, String tableName) {
	    boolean exists;
	    try {
		DescribeTableRequest req = DescribeTableRequest.builder()
		    .tableName(tableName)
		    .build();
		dynamoDbClient.describeTable(req);
		exists = true;
	    } catch (ResourceNotFoundException ex) {
		exists = false;
	    }
	    return exists;
	}

	public void create () {
	    HashMap<String,AttributeValue> item = new HashMap<String,AttributeValue>();
	    item.put("sessionKey", AttributeValue.builder().s(sessionKey).build());
	    item.put("createdAt",AttributeValue.builder()
		     .n(Long.toString(System.currentTimeMillis())).build());
	    item.put("files", AttributeValue.builder().l(new ArrayList<AttributeValue>()).build());
	    item.put("ownerKey", AttributeValue.builder().nul(true).build());
	    item.put("uploads", AttributeValue.builder().m(new HashMap<String,AttributeValue>()).build());
	    item.put("locked", AttributeValue.builder().bool(false).build());

	    PutItemRequest req = PutItemRequest.builder()
		.tableName(tableName)
		.item(item)
		.build();

	    dynamoDbClient.putItem(req);
	}

	private Map<String,AttributeValue> getItemRequestKey() {
	    Map<String,AttributeValue> key = new HashMap<String,AttributeValue>();
	    key.put("sessionKey", AttributeValue.builder().s(sessionKey).build());
	    return key;
	}

	public boolean exists () {
	    GetItemRequest req = GetItemRequest.builder()
                .tableName(tableName)
                .key(getItemRequestKey())
		.projectionExpression("sessionKey")
                .build();

	    GetItemResponse res = dynamoDbClient.getItem(req);
	    return res.hasItem();
	}

	public void setOwnerKey (String ownerKey) {
	    Map<String,AttributeValue> attributeValues = new HashMap <String,AttributeValue>();
	    attributeValues.put(":ownerKey", AttributeValue.builder().s(ownerKey).build());
	    
	    UpdateItemRequest req = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(getItemRequestKey())
		.updateExpression("SET ownerKey = :ownerKey")
		.expressionAttributeValues(attributeValues)
                .build();

	    dynamoDbClient.updateItem(req);
	}

	public boolean matchOwnerKey (String ownerKey) {
	    GetItemRequest req = GetItemRequest.builder()
                .tableName(tableName)
                .key(getItemRequestKey())
		.projectionExpression("ownerKey")
                .build();

	    GetItemResponse res = dynamoDbClient.getItem(req);
	    return res.item().get("ownerKey").s().equals(ownerKey);
	}
	
	public int appendFile (String fileName, String contentType) {
	    Map<String,AttributeValue> file = new HashMap <String,AttributeValue>();
	    file.put("fileName", AttributeValue.builder().s(fileName).build());
	    file.put("contentType", AttributeValue.builder().s(contentType).build());
	    file.put("uploadId", AttributeValue.builder().nul(true).build());

	    List<AttributeValue> files = new ArrayList <AttributeValue>();
	    files.add(AttributeValue.builder().m(file).build());
	    
	    Map<String,AttributeValue> attributeValues = new HashMap <String,AttributeValue>();
	    attributeValues.put(":files", AttributeValue.builder().l(files).build());

	    UpdateItemRequest req = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(getItemRequestKey())
		.updateExpression("SET files = list_append(files,:files)")
		.expressionAttributeValues(attributeValues)
		.returnValues(ReturnValue.UPDATED_NEW)
                .build();

	    UpdateItemResponse res = dynamoDbClient.updateItem(req);
	    return res.attributes().get("files").l().size()-1; // [MEMO] return last index for new fileId
	}

	public int getFileCount () {
	    GetItemRequest req = GetItemRequest.builder()
                .tableName(tableName)
                .key(getItemRequestKey())
		.projectionExpression("files")
                .build();

	    GetItemResponse res = dynamoDbClient.getItem(req);
	    return res.item().get("files").l().size();
	}
	
	public void putUploadIdForFileId (int fileId, String uploadId) {
	    Map<String,String> attributeNames = new HashMap <String,String>();
	    attributeNames.put("#uploadId",uploadId);

	    Map<String,AttributeValue> attributeValues = new HashMap <String,AttributeValue>();
	    attributeValues.put(":uploadId", AttributeValue.builder().s(uploadId).build());

	    Map<String,AttributeValue> upload = new HashMap <String,AttributeValue>();
	    upload.put("etags", AttributeValue.builder().l(new ArrayList<AttributeValue>()).build());
	    upload.put("fileSize", AttributeValue.builder().n("0").build());

	    attributeValues.put(":upload", AttributeValue.builder().m(upload).build());

	    UpdateItemRequest req = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(getItemRequestKey())
		.updateExpression("SET files[" + Integer.toString(fileId) + "].uploadId = :uploadId, uploads.#uploadId = :upload")
		.expressionAttributeNames(attributeNames)
		.expressionAttributeValues(attributeValues)
                .build();

	    dynamoDbClient.updateItem(req);
	}
	
	public String getUploadIdForFileId (int fileId) {
	    GetItemRequest req = GetItemRequest.builder()
                .tableName(tableName)
                .key(getItemRequestKey())
		.projectionExpression("files[" + Integer.toString(fileId) + "].uploadId")
                .build();

	    GetItemResponse res = dynamoDbClient.getItem(req);
	    return res.item().get("files").l().get(0).m().get("uploadId").s(); // [MEMO] 1st file in result
	}

	public void addEtagForUploadId (String uploadId, String etag) {
	    Map<String,String> attributeNames = new HashMap <String,String>();
	    attributeNames.put("#uploadId",uploadId);

	    List<AttributeValue> etags = new ArrayList <AttributeValue>();
	    etags.add(AttributeValue.builder().s(etag).build());

	    Map<String,AttributeValue> attributeValues = new HashMap <String,AttributeValue>();
	    attributeValues.put(":etags", AttributeValue.builder().l(etags).build());

	    UpdateItemRequest req = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(getItemRequestKey())
		.updateExpression("SET uploads.#uploadId.etags = list_append(uploads.#uploadId.etags,:etags)")
		.expressionAttributeNames(attributeNames)
		.expressionAttributeValues(attributeValues)
                .build();

	    dynamoDbClient.updateItem(req);
	}
	public List<String> getEtagsForFileId (int fileId) {
	    String uploadId = getUploadIdForFileId (fileId);
	    if (uploadId == null) {
		return null;
	    }

	    Map<String,String> attributeNames = new HashMap <String,String>();
	    attributeNames.put("#uploadId",uploadId);

	    GetItemRequest req = GetItemRequest.builder()
		.tableName(tableName)
		.key(getItemRequestKey())
		.projectionExpression("uploads.#uploadId.etags")
		.expressionAttributeNames(attributeNames)
		.build();

	    GetItemResponse res = dynamoDbClient.getItem(req);
	    List<AttributeValue> etagsa = res.item().get("uploads").m().get(uploadId).m().get("etags").l();

	    List<String> etags = new ArrayList<String>();
	    for(AttributeValue av: etagsa) {
		etags.add(av.s());
	    }
	    return etags;
	}

	public void putFileSizeForUploadId (String uploadId, long fileSize) {
	    Map<String,String> attributeNames = new HashMap <String,String>();
	    attributeNames.put("#uploadId",uploadId);

	    Map<String,AttributeValue> attributeValues = new HashMap <String,AttributeValue>();
	    attributeValues.put(":fileSize", AttributeValue.builder().n(Long.toString(fileSize)).build());

	    UpdateItemRequest req = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(getItemRequestKey())
		.updateExpression("SET uploads.#uploadId.fileSize = :fileSize")
		.expressionAttributeNames(attributeNames)
		.expressionAttributeValues(attributeValues)
                .build();

	    dynamoDbClient.updateItem(req);
	}
	public Long getFileSizeForFileId (int fileId) {
	    String uploadId = getUploadIdForFileId (fileId);
	    if (uploadId == null) {
		return null;
	    }

	    Map<String,String> attributeNames = new HashMap <String,String>();
	    attributeNames.put("#uploadId",uploadId);

	    GetItemRequest req = GetItemRequest.builder()
		.tableName(tableName)
		.key(getItemRequestKey())
		.projectionExpression("uploads.#uploadId.fileSize")
		.expressionAttributeNames(attributeNames)
		.build();

	    GetItemResponse res = dynamoDbClient.getItem(req);
	    return Long.valueOf(res.item().get("uploads").m().get(uploadId).m().get("fileSize").n());
	}

	public List<FileListItem> getFileList () {
	    GetItemRequest req = GetItemRequest.builder()
                .tableName(tableName)
                .key(getItemRequestKey())
		.projectionExpression("files")
                .build();

	    GetItemResponse res = dynamoDbClient.getItem(req);
	    List<AttributeValue> files = res.item().get("files").l();
	    
	    List<FileListItem> result = new ArrayList<FileListItem>();
	    for( AttributeValue file : files) {
		Map<String,AttributeValue> m = file.m();
		String fileName = m.get("fileName").s();
		String contentType = m.get("contentType").s();
		FileListItem item = new FileListItem(fileName,contentType);
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
	    GetItemRequest req = GetItemRequest.builder()
                .tableName(tableName)
                .key(getItemRequestKey())
		.projectionExpression("files[" + Integer.toString(fileId) + "]")
                .build();

	    GetItemResponse res = dynamoDbClient.getItem(req);
	    AttributeValue file = res.item().get("files").l().get(0); // [MEMO] 1st file in result
	    Map<String,AttributeValue> m = file.m();
	    String fileName = m.get("fileName").s();
	    String contentType = m.get("contentType").s();
	    return new FileListItem(fileName,contentType);
	}

	public void lock () {
	    Map<String,AttributeValue> attributeValues = new HashMap <String,AttributeValue>();
	    attributeValues.put(":lock", AttributeValue.builder().bool(true).build());
	    
	    UpdateItemRequest req = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(getItemRequestKey())
		.updateExpression("SET locked = :lock")
		.expressionAttributeValues(attributeValues)
                .build();

	    dynamoDbClient.updateItem(req);
	}
	
	public boolean locked () {
	    GetItemRequest req = GetItemRequest.builder()
                .tableName(tableName)
                .key(getItemRequestKey())
		.projectionExpression("locked")
                .build();

	    GetItemResponse res = dynamoDbClient.getItem(req);
	    return res.item().get("locked").bool();
	}
	
	public void delete () {
	    DeleteItemRequest req = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(getItemRequestKey())
                .build();

	    dynamoDbClient.deleteItem(req);
	}
	
    }


    private static class BlobManager {
	private S3Client s3Client;
	private String bucket;
	private String sessionKey;
	public BlobManager (S3Client s3Client, String bucket, String sessionKey) {
	    this.s3Client = s3Client;
	    this.bucket = bucket;
	    this.sessionKey = sessionKey;
	}

	private String getFileDataFilePath(int fileId) {
	    return sessionKey + "/" + Integer.toString(fileId);
	}

	public String createMultiPart(int fileId) {
	    CreateMultipartUploadRequest multipartReq = CreateMultipartUploadRequest.builder()
		.bucket(bucket)
		.key(getFileDataFilePath(fileId))
		.build();
	    CreateMultipartUploadResponse multipartRes = s3Client.createMultipartUpload(multipartReq);
	    return multipartRes.uploadId();
	}
	public String uploadPart (int fileId, String uploadId, int partNumber, InputStream in, long len) {
	    UploadPartRequest req = UploadPartRequest.builder()
		.bucket(bucket)
		.key(getFileDataFilePath(fileId))
		.uploadId(uploadId)
		.partNumber(partNumber+1) // [MEMO] partNumber starts from 1
		.build();
	    UploadPartResponse res = s3Client.uploadPart(req, RequestBody.fromInputStream(in,len));
	    return res.eTag();
	}
	public void completeMultiPart(int fileId, String uploadId, List<String> etags) {
	    ArrayList<CompletedPart> parts = new ArrayList<CompletedPart>();
	    for (int i = 0; i < etags.size(); i++) {
		String etag = etags.get(i);
		CompletedPart part = CompletedPart.builder()
		    .partNumber(i+1) // [MEMO] partNumber starts from 1
		    .eTag(etag)
		    .build();
		parts.add(part);
	    }

	    CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
		.parts(parts)
		.build();
	    
	    CompleteMultipartUploadRequest req = CompleteMultipartUploadRequest.builder()
		.bucket(bucket)
		.key(getFileDataFilePath(fileId))
		.uploadId(uploadId)
		.multipartUpload(completedMultipartUpload)
		.build();
	    s3Client.completeMultipartUpload(req);
	}

	public InputStream getFileDataInputStream (int fileId) {
	    GetObjectRequest req = GetObjectRequest.builder()
		.bucket(bucket)
		.key(getFileDataFilePath(fileId))
		.build();
	    return s3Client.getObject(req);
	}
	public long getFileSize(int fileId) {
	    HeadObjectRequest req = HeadObjectRequest.builder()
		.bucket(bucket)
		.key(getFileDataFilePath(fileId))
		.build();
	    HeadObjectResponse res = s3Client.headObject(req);
	    return res.contentLength();
	}

	public void deleteAll() {
	    ListObjectsRequest listReq = ListObjectsRequest.builder()
		.bucket(bucket)
		.prefix(sessionKey + "/")
		.build();
	    ListObjectsResponse listRes = s3Client.listObjects(listReq);
	    for(S3Object obj : listRes.contents()) {
		DeleteObjectRequest req = DeleteObjectRequest.builder()
		    .bucket(bucket)
		    .key(obj.key())
		    .build();
		s3Client.deleteObject(req);
	    }
	    
	}

    }


    private DynamoDbClient dynamoDbClient;
    private String dynamoTable;
    private S3Client s3Client;
    private String s3Bucket;
    private int maxFileCount;
    private long maxFileSize;
    public AwsS3Storage (String region, String accessKeyId, String secretAccessKey, String dynamoTable, String s3Bucket, int maxFileCount, long maxFileSize) {
	AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
	dynamoDbClient = DynamoDbClient.builder()
	    .region(Region.of(region))
	    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
	    .build();
	s3Client = S3Client.builder()
	    .region(Region.of(region))
	    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
	    .build();
	this.s3Bucket = s3Bucket;
	this.dynamoTable = dynamoTable;
	this.maxFileCount = maxFileCount;
	this.maxFileSize = maxFileSize;
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
	String sessionKey = Util.genAlphaNumericKey(16);
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

}
