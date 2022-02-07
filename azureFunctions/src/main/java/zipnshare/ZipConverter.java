package zipnshare;

import java.util.Map;
import java.util.logging.Level;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;

import type.BackgroundJob;
import azure.AzureBlobBackgroundJobV12;

/**
 * Azure Functions with Azure Storage Queue trigger.
 */
public class ZipConverter {
	/**
	 * This function will be invoked when a new message is received at the specified path. The message contents are provided as input to this function.
	 */
	@FunctionName("ZipConverter")
	public void run(
					@QueueTrigger(name = "message", queueName = "zipnshare", connection = "ZIPNSHARE_STORAGE_ACCOUNT_CS") String message,
					final ExecutionContext context
					) {
		context.getLogger().info("Java Queue trigger function processed a message: " + message);

		Map<String,String> env = System.getenv();
		String cosmosAccountEndpoint = env.get("ZIPNSHARE_COSMOS_ACCOUNT_ENDPOINT");
		String cosmosAccountKey      = env.get("ZIPNSHARE_COSMOS_ACCOUNT_KEY");
		String cosmosDatabase        = env.get("ZIPNSHARE_COSMOS_DATABASE");
		String storageAccountCS      = env.get("ZIPNSHARE_STORAGE_ACCOUNT_CS");
		String blobServiceContainer  = env.get("ZIPNSHARE_BLOB_SERVICE_CONTAINER");

//		context.getLogger().info("cosmosAccountEndpoint: " + cosmosAccountEndpoint);
//		context.getLogger().info("cosmosAccountKey: " + cosmosAccountKey);
//		context.getLogger().info("cosmosDatabase: " + cosmosDatabase);
//		context.getLogger().info("storageAccountCS: " + storageAccountCS);
//		context.getLogger().info("blobServiceContainer: " + blobServiceContainer);

		String sessionKey = message;
		BackgroundJob backgroundJob = new AzureBlobBackgroundJobV12(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase, storageAccountCS, blobServiceContainer);
		try {
			backgroundJob.zipConvert(sessionKey);
//		} catch (BackgroundJob.NoSuchSessionException ex) {
//			// [MEMO] to delete from queue
//		} catch (Exception ex) {
//			throw new RuntimeException ("failed to zipConvert", ex);
		} catch (Exception ex) {
			// [MEMO] allways delete from queue when fail
			// [TODO] log
			context.getLogger().log(Level.WARNING,"failed to zipConvert", ex);
		}

	}
}
