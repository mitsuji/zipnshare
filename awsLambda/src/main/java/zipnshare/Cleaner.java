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

public class Cleaner implements RequestHandler<ScheduledEvent, Void>{
  private static final Logger logger = LoggerFactory.getLogger(ZipConverter.class);
  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private static final LambdaAsyncClient lambdaClient = LambdaAsyncClient.create();
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
//    // call Lambda API
//    logger.info("Getting account settings");
//    CompletableFuture<GetAccountSettingsResponse> accountSettings = 
//        lambdaClient.getAccountSettings(GetAccountSettingsRequest.builder().build());
//    // log execution details
//    logger.info("ENVIRONMENT VARIABLES: {}", gson.toJson(System.getenv()));
//    logger.info("CONTEXT: {}", gson.toJson(context));
//    logger.info("EVENT: {}", gson.toJson(event));
    // process event

    logger.info("fooooooooooooooo");

//    // process Lambda API response
//    try {
//      GetAccountSettingsResponse settings = accountSettings.get();
//      response = gson.toJson(settings.accountUsage());
//      logger.info("Account usage: {}", response);
//    } catch(Exception e) {
//      e.getStackTrace();
//    }
//    return response;
    return null;
  }
}
