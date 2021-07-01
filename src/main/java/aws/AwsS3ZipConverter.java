package aws;

import java.util.List;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

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
		System.out.println("messageId: " + msg.messageId());
		System.out.println("receiptHandle: " + msg.receiptHandle());
		
//		DeleteMessageRequest reqDel = DeleteMessageRequest.builder()
//		    .queueUrl(sqsUrl)
//		    .receiptHandle(msg.receiptHandle())
//		    .build();
//		sqsClient.deleteMessage(reqDel);
	    }
	    
	    try {
		Thread.sleep (500);
	    } catch (InterruptedException ex) {
		break;
	    }
	}
    }

}

