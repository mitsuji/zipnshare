package zipnshare;

import java.util.Map;

import java.time.LocalDateTime;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;

import azure.AzureBlobBackgroundJobV12;

/**
 * Azure Functions with Timer trigger.
 */
public class Cleaner {
    /**
     * This function will be invoked periodically according to the specified schedule.
     */
    @FunctionName("Cleaner")
    public void run(
        @TimerTrigger(name = "timerInfo", schedule = "0 */2 * * * *") String timerInfo,
        final ExecutionContext context
    ) {
        context.getLogger().info("Java Timer trigger function executed at: " + LocalDateTime.now());

	Map<String,String> env = System.getenv();
	String cosmosAccountEndpoint = env.get("ZIPNSHARE_COSMOS_ACCOUNT_ENDPOINT");
	String cosmosAccountKey      = env.get("ZIPNSHARE_COSMOS_ACCOUNT_KEY");
	String cosmosDatabase        = env.get("ZIPNSHARE_COSMOS_DATABASE");
	String storageAccountCS      = env.get("ZIPNSHARE_STORAGE_ACCOUNT_CS");
	String blobServiceContainer  = env.get("ZIPNSHARE_BLOB_SERVICE_CONTAINER");
	long cleanExpiredSeconds     = Long.valueOf(env.get("ZIPNSHARE_CLEAN_EXPIRED_SECONDS"));
	long cleanGarbageSeconds     = Long.valueOf(env.get("ZIPNSHARE_CLEAN_GARBAGE_SECONDS"));

//	context.getLogger().info("cosmosAccountEndpoint: " + cosmosAccountEndpoint);
//	context.getLogger().info("cosmosAccountKey: " + cosmosAccountKey);
//	context.getLogger().info("cosmosDatabase: " + cosmosDatabase);
//	context.getLogger().info("storageAccountCS: " + storageAccountCS);
//	context.getLogger().info("blobServiceContainer: " + blobServiceContainer);

	AzureBlobBackgroundJobV12 backgroundJob = new AzureBlobBackgroundJobV12(cleanExpiredSeconds, cleanGarbageSeconds,
						cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase, storageAccountCS, blobServiceContainer);
	try {
	    backgroundJob.clean();
	} catch (Exception ex) {
	    throw new RuntimeException ("failed to clean", ex);
	}

    }
}
