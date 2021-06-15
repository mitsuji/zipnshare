package aws;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import type.FileListItem;

public class DatabaseManager {

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
	    item.put("ziped", AttributeValue.builder().bool(false).build());

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
	
	public boolean ziped () {
	    GetItemRequest req = GetItemRequest.builder()
            .tableName(tableName)
            .key(getItemRequestKey())
		.projectionExpression("ziped")
            .build();

	    GetItemResponse res = dynamoDbClient.getItem(req);
	    return res.item().get("ziped").bool();
	}
	
	public void zip () {
	    Map<String,AttributeValue> attributeValues = new HashMap <String,AttributeValue>();
	    attributeValues.put(":zip", AttributeValue.builder().bool(true).build());
	    
	    UpdateItemRequest req = UpdateItemRequest.builder()
            .tableName(tableName)
            .key(getItemRequestKey())
		.updateExpression("SET ziped = :zip")
		.expressionAttributeValues(attributeValues)
            .build();

	    dynamoDbClient.updateItem(req);
	}
	
}


