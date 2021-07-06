package aws;

import java.util.List;
import java.util.Map;

import java.io.IOException;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.paginators.*;

import software.amazon.awssdk.services.s3.S3Client;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import org.mitsuji.vswf.ZipWriter;
import type.FileListItem;
import type.BackgroundJob;

public class AwsS3BackgroundJob implements BackgroundJob {

    private long cleanExpiredSeconds;
    private long cleanGarbageSeconds;
    private DynamoDbClient dynamoDbClient;
    private String dynamoTable;
    private S3Client s3Client;
    private String s3Bucket;
    public AwsS3BackgroundJob (String region, String accessKeyId, String secretAccessKey, String dynamoTable, String s3Bucket) {
	this (0,0,region,accessKeyId,secretAccessKey,dynamoTable,s3Bucket);
    }

    public AwsS3BackgroundJob (long cleanExpiredSeconds, long cleanGarbageSeconds,
			       String region, String accessKeyId, String secretAccessKey, String dynamoTable, String s3Bucket) {
	this.cleanExpiredSeconds = cleanExpiredSeconds;
	this.cleanGarbageSeconds = cleanGarbageSeconds;
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
    }

    public void clean () throws BackgroundJobException {
	ScanRequest scanReq = ScanRequest.builder()
	    .tableName(dynamoTable)
	    .limit(100)
	    .projectionExpression("sessionKey")
	    .build();
	ScanIterable scanIterable = dynamoDbClient.scanPaginator(scanReq);
	for (ScanResponse response : scanIterable) {
	    for (Map<String,AttributeValue> item : response.items()) {
		String sessionKey = item.get("sessionKey").s();
		DatabaseManager dm = new DatabaseManager(dynamoDbClient,dynamoTable,sessionKey);
		BlobManager bm = new BlobManager (s3Client,s3Bucket,sessionKey);
		long now = System.currentTimeMillis();
		long createdAt = dm.getCreatedAt();
		boolean locked = dm.locked();
		boolean expired1 = createdAt + (cleanExpiredSeconds * 1000) < now; // expired
		boolean expired2 = (!locked) && (createdAt + (cleanGarbageSeconds * 1000) < now); // garbage
		if (expired1 || expired2) {
		    bm.deleteAll();
		    dm.delete();
		}
	    }
	}
    }

    public void zipConvert (String sessionKey) throws BackgroundJobException {
	DatabaseManager dm = new DatabaseManager(dynamoDbClient,dynamoTable,sessionKey);
	BlobManager bm = new BlobManager (s3Client,s3Bucket,sessionKey);

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
