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

import org.mitsuji.vswf.ZipWriter;
import type.FileListItem;

public class AwsS3Cleaner implements Runnable {

    private DynamoDbClient dynamoDbClient;
    private String dynamoTable;
    private S3Client s3Client;
    private String s3Bucket;
    public AwsS3Cleaner (String region, String accessKeyId, String secretAccessKey, String dynamoTable, String s3Bucket) {
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

    public void run () {
	while (true) {
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
		    boolean expired1 = createdAt + (7 * 24 * 60 * 60 * 1000) < now; // expired
//		    boolean expired1 = createdAt + (5 * 60 * 1000) < now; // expired (test)
		    boolean expired2 = (!locked) && (createdAt + (1 * 60 * 60 * 1000) < now); // gabage
		    if (expired1 || expired2) {
			bm.deleteAll();
			dm.delete();
		    }
		}
	    }
	    try {
		Thread.sleep (500);
	    } catch (InterruptedException ex) {
		break;
	    }
	}
    }

}

