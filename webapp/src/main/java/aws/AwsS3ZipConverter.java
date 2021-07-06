package aws;

import java.util.List;

import java.io.IOException;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import type.BackgroundJob;

public class AwsS3ZipConverter implements Runnable {

    private long zipConvertIntervalSeconds;
    private SqsClient sqsClient;
    private String sqsUrl;
    private String sqsGroupId;
    private AwsS3BackgroundJob backgroundJob;
    public AwsS3ZipConverter (long zipConvertIntervalSeconds,
			      String region, String accessKeyId, String secretAccessKey, String dynamoTable, String s3Bucket, String sqsUrl, String sqsGroupId) {
	this.zipConvertIntervalSeconds = zipConvertIntervalSeconds;
	AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
	sqsClient = SqsClient.builder()
	    .region(Region.of(region))
	    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
	    .build();
	this.sqsUrl = sqsUrl;
	this.sqsGroupId = sqsGroupId;
	backgroundJob = new AwsS3BackgroundJob(region, accessKeyId, secretAccessKey, dynamoTable, s3Bucket);
    }

    public void run () {
	while (true) {
	    try {
		ReceiveMessageRequest req = ReceiveMessageRequest.builder()
		    .queueUrl(sqsUrl)
		    .maxNumberOfMessages(1)
		    .build();
		ReceiveMessageResponse res = sqsClient.receiveMessage(req);
		List<Message> messages = res.messages();
		if (res.hasMessages()) {
		    Message msg = messages.get(0);
//		    System.out.println("messageBody: " + msg.body());
//		    System.out.println("messageId: " + msg.messageId());
//		    System.out.println("receiptHandle: " + msg.receiptHandle());
		    String sessionKey = msg.body();

		    // [MEMO] extend visibility timeout
		    ChangeMessageVisibilityRequest reqCMV = ChangeMessageVisibilityRequest.builder()
			.queueUrl(sqsUrl)
			.receiptHandle(msg.receiptHandle())
			.visibilityTimeout(30*60) // 30 minutes
			.build();
		    sqsClient.changeMessageVisibility(reqCMV);

		    boolean succeed = false;
		    try {
			backgroundJob.zipConvert(sessionKey);
			succeed = true;
		    } catch (BackgroundJob.BackgroundJobException ex) {
			// [TODO] log
			ex.printStackTrace();
		    }

		    if (succeed) {
			// [MEMO] delete message
			DeleteMessageRequest reqDel = DeleteMessageRequest.builder()
			    .queueUrl(sqsUrl)
			    .receiptHandle(msg.receiptHandle())
			    .build();
			sqsClient.deleteMessage(reqDel);
		    }
		}
		Thread.sleep (zipConvertIntervalSeconds * 1000);
	    } catch (InterruptedException ex) {
		break;
	    } catch (Exception ex) {
		// [TODO] log
		ex.printStackTrace();
	    }
	}
    }

}

