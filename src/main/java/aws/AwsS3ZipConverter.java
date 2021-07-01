package aws;

import java.util.List;

import java.io.IOException;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import org.mitsuji.vswf.ZipWriter;
import type.FileListItem;

public class AwsS3ZipConverter implements Runnable {

    private DynamoDbClient dynamoDbClient;
    private String dynamoTable;
    private S3Client s3Client;
    private String s3Bucket;
    private SqsClient sqsClient;
    private String sqsUrl;
    private String sqsGroupId;
    public AwsS3ZipConverter (String region, String accessKeyId, String secretAccessKey, String dynamoTable, String s3Bucket, String sqsUrl, String sqsGroupId) {
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
	sqsClient = SqsClient.builder()
	    .region(Region.of(region))
	    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
	    .build();
	this.sqsUrl = sqsUrl;
	this.sqsGroupId = sqsGroupId;
    }

    public void run () {
	while (true) {
	    ReceiveMessageRequest req = ReceiveMessageRequest.builder()
		.queueUrl(sqsUrl)
		.maxNumberOfMessages(1)
		.build();
	    ReceiveMessageResponse res = sqsClient.receiveMessage(req);
	    List<Message> messages = res.messages();
	    if (res.hasMessages()) {
		Message msg = messages.get(0);
		System.out.println("messageBody: " + msg.body());
//		System.out.println("messageId: " + msg.messageId());
//		System.out.println("receiptHandle: " + msg.receiptHandle());
		String sessionKey = msg.body();
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
		    dm.zip();
		    // [TODO] msg.receiptHandle() timeouts
		    DeleteMessageRequest reqDel = DeleteMessageRequest.builder()
			.queueUrl(sqsUrl)
			.receiptHandle(msg.receiptHandle())
			.build();
		    sqsClient.deleteMessage(reqDel);
		} catch (IOException ex) {
		    // [TODO] log
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

