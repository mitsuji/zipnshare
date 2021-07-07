package aws;

import java.util.List;

import java.io.IOException;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import type.BackgroundJob;
import type.ZipQueueProcessor;

public class AwsS3ZipQueueProcessor implements ZipQueueProcessor {

    private BackgroundJob backgroundJob;
    private SqsClient sqsClient;
    private String sqsUrl;
    private String sqsGroupId;
    public AwsS3ZipQueueProcessor(String region, String accessKeyId, String secretAccessKey,
				  String dynamoTable, String s3Bucket,
				  String sqsUrl, String sqsGroupId ) {
	backgroundJob = new AwsS3BackgroundJob(region, accessKeyId, secretAccessKey, dynamoTable, s3Bucket);
	AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
	sqsClient = SqsClient.builder()
	    .region(Region.of(region))
	    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
	    .build();
	this.sqsUrl = sqsUrl;
	this.sqsGroupId = sqsGroupId;
    }

    public void process () throws Exception {
	ReceiveMessageRequest req = ReceiveMessageRequest.builder()
	    .queueUrl(sqsUrl)
	    .maxNumberOfMessages(1)
	    .build();
	ReceiveMessageResponse res = sqsClient.receiveMessage(req);
	List<Message> messages = res.messages();
	if (res.hasMessages()) {
	    Message msg = messages.get(0);
//	    System.out.println("messageBody: " + msg.body());
//	    System.out.println("messageId: " + msg.messageId());
//	    System.out.println("receiptHandle: " + msg.receiptHandle());
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
	    } catch (BackgroundJob.NoSuchSessionException ex) {
		// [TODO] log
		ex.printStackTrace();
		succeed = true; // [MEMO] to delete from queue
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

    }

}

