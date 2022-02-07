package zipnshare;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;

import software.amazon.awssdk.services.lambda.model.GetAccountSettingsRequest;
import software.amazon.awssdk.services.lambda.model.GetAccountSettingsResponse;
import software.amazon.awssdk.services.lambda.model.ServiceException;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.AccountUsage;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.StringBuilder;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import java.io.StringWriter;
import java.io.PrintWriter;

import type.BackgroundJob;
import aws.AwsS3BackgroundJob;

public class ZipConverter implements RequestHandler<SQSEvent, Void>{
//	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
//	private static final LambdaAsyncClient lambdaClient = LambdaAsyncClient.create();
	public ZipConverter(){
//		CompletableFuture<GetAccountSettingsResponse> accountSettings = lambdaClient.getAccountSettings(GetAccountSettingsRequest.builder().build());
//		try {
//			GetAccountSettingsResponse settings = accountSettings.get();
//		} catch(Exception e) {
//			e.getStackTrace();
//		}
	}
	@Override
	public Void handleRequest(SQSEvent event, Context context)
	{
		Map<String,String> env = System.getenv();
		String region = env.get("ZIPNSHARE_AWS_REGION");
		String accessKeyId = env.get("ZIPNSHARE_ACCESS_KEY_ID");
		String secretAccessKey = env.get("ZIPNSHARE_SECRET_KEY");
		String dynamoTable = env.get("ZIPNSHARE_DYNAMO_TABLE");
		String s3Bucket = env.get("ZIPNSHARE_S3_BUCKET");
		String sqsUrl = env.get("ZIPNSHARE_SQS_URL");

//		logger.info("region: " + region);
//		logger.info("accessKeyId: " + accessKeyId);
//		logger.info("secretAccessKey: " + secretAccessKey);
//		logger.info("dynamoTable: " + dynamoTable);
//		logger.info("s3Bucket: " + s3Bucket);

		BackgroundJob backgroundJob = new AwsS3BackgroundJob(region, accessKeyId, secretAccessKey, dynamoTable, s3Bucket);
		AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
		SqsClient sqsClient = SqsClient.builder()
			.region(Region.of(region))
			.credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
			.build();
		for(SQSMessage msg : event.getRecords()){

			// [MEMO] extend visibility timeout
			ChangeMessageVisibilityRequest reqCMV = ChangeMessageVisibilityRequest.builder()
				.queueUrl(sqsUrl)
				.receiptHandle(msg.getReceiptHandle())
				.visibilityTimeout(30*60) // 30 minutes
				.build();
			sqsClient.changeMessageVisibility(reqCMV);

			String sessionKey = msg.getBody();
			//	  logger.info("sessionKey: " + sessionKey);
			try {
				backgroundJob.zipConvert(sessionKey);
//			} catch (BackgroundJob.NoSuchSessionException ex) {
//				// [MEMO] to delete from queue
//			} catch (Exception ex) {
//				throw new RuntimeException ("failed to zipConvert", ex);
			} catch (Exception ex) {
				// [MEMO] allways delete from queue when fail
				StringWriter stringWriter = new StringWriter();
				PrintWriter writer = new PrintWriter(stringWriter);
				writer.println("failed to zipConvert: ");
				ex.printStackTrace(writer);
				context.getLogger().log(stringWriter.toString());
			}
		}
		return null;
	}
}
