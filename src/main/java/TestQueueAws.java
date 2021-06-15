import java.util.List;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

public class TestQueueAws {

    public static void main (String [] args) {
//	enqueue(args);
	dequeue(args);
    }

    private static void enqueue (String [] args) {
	AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(args[0], args[1]);
	SqsClient sqsClient = SqsClient.builder()
	    .region(Region.of("us-east-2"))
	    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
	    .build();
	SendMessageRequest req = SendMessageRequest.builder()
	    .queueUrl(args[2])
	    .messageGroupId("main")
	    .messageBody("foooo003")
	    .build();
	sqsClient.sendMessage(req);
    }

    private static void dequeue (String [] args) {
	AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(args[0], args[1]);
	SqsClient sqsClient = SqsClient.builder()
	    .region(Region.of("us-east-2"))
	    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
	    .build();
	ReceiveMessageRequest req = ReceiveMessageRequest.builder()
	    .queueUrl(args[2])
	    .maxNumberOfMessages(1)
	    .build();
	ReceiveMessageResponse res = sqsClient.receiveMessage(req);
	List<Message> messages = res.messages();
	if (res.hasMessages()) {
	    Message msg = messages.get(0);
	    System.out.println("messageBody: " + msg.body());
	    System.out.println("messageId: " + msg.messageId());
	    System.out.println("receiptHandle: " + msg.receiptHandle());

	    DeleteMessageRequest reqDel = DeleteMessageRequest.builder()
		.queueUrl(args[2])
		.receiptHandle(msg.receiptHandle())
		.build();
	    sqsClient.deleteMessage(reqDel);
	} else {
	    System.out.println("empty queue");
	}
    }

    
}
