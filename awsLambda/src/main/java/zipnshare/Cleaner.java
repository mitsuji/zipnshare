package zipnshare;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;

import software.amazon.awssdk.services.lambda.model.GetAccountSettingsRequest;
import software.amazon.awssdk.services.lambda.model.GetAccountSettingsResponse;
import software.amazon.awssdk.services.lambda.model.ServiceException;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.AccountUsage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.StringBuilder;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import aws.AwsS3BackgroundJob;

public class Cleaner implements RequestHandler<ScheduledEvent, Void>{
  private static final Logger logger = LoggerFactory.getLogger(ZipConverter.class);
//  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
//  private static final LambdaAsyncClient lambdaClient = LambdaAsyncClient.create();
  public Cleaner(){
//    CompletableFuture<GetAccountSettingsResponse> accountSettings = lambdaClient.getAccountSettings(GetAccountSettingsRequest.builder().build());
//    try {
//      GetAccountSettingsResponse settings = accountSettings.get();
//    } catch(Exception e) {
//      e.getStackTrace();
//    }
  }
  @Override
  public Void handleRequest(ScheduledEvent event, Context context)
  {
      Map<String,String> env = System.getenv();
      String region = env.get("ZIPNSHARE_AWS_REGION");
      String accessKeyId = env.get("AWS_ACCESS_KEY_ID");
      String secretAccessKey = env.get("AWS_SECRET_KEY");
      String dynamoTable = env.get("ZIPNSHARE_DYNAMO_TABLE");
      String s3Bucket = env.get("ZIPNSHARE_S3_BUCKET");
      
      logger.info("region: " + region);
      logger.info("accessKeyId: " + accessKeyId);
      logger.info("secretAccessKey: " + secretAccessKey);
      logger.info("dynamoTable: " + dynamoTable);
      logger.info("s3Bucket: " + s3Bucket);

      AwsS3BackgroundJob backgroundJob = new AwsS3BackgroundJob(region, accessKeyId, secretAccessKey, dynamoTable, s3Bucket);
      try {
	  backgroundJob.clean();
      } catch (Exception ex) {
	  throw new RuntimeException ("failed to clean", ex);
      }

    return null;
  }
}
