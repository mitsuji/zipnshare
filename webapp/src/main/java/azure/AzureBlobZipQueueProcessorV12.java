package azure;

import com.azure.storage.queue.*;
import com.azure.storage.queue.models.*;

import java.util.Base64;

import type.BackgroundJob;
import type.ZipQueueProcessor;

public class AzureBlobZipQueueProcessorV12 implements ZipQueueProcessor {

    private BackgroundJob backgroundJob;
    private QueueServiceClient queueServiceClient;
    private String queueName;

    public AzureBlobZipQueueProcessorV12(String cosmosAccountEndpoint, String cosmosAccountKey, String cosmosDatabase,
            String storageAccountCS, String blobServiceContainer, String queueName) {
        backgroundJob = new AzureBlobBackgroundJobV12(cosmosAccountEndpoint, cosmosAccountKey, cosmosDatabase,
                storageAccountCS, blobServiceContainer);
        queueServiceClient = new QueueServiceClientBuilder().connectionString(storageAccountCS).buildClient();
        this.queueName = queueName;
    }

    public void process() throws Exception {
        QueueClient queueClient = queueServiceClient.getQueueClient(queueName);
        QueueMessageItem item = queueClient.receiveMessage();
        if (item != null) {
            // System.out.println("messageBody: " + item.getBody().toString());
            // System.out.println("messageId: " + item.getMessageId());
            // System.out.println("popReceipt: " + item.getPopReceipt());
            String sessionKeyBE64 = item.getBody().toString();
            String sessionKey = new String(Base64.getDecoder().decode(sessionKeyBE64), "UTF-8");

            // [MEMO] needs extend visibility timeout ?

            boolean succeed = false;
            try {
                backgroundJob.zipConvert(sessionKey);
                succeed = true;
                // } catch (BackgroundJob.NoSuchSessionException ex) {
                // // [TODO] log
                // ex.printStackTrace();
                // succeed = true; // [MEMO] to delete from queue
                // } catch (BackgroundJob.BackgroundJobException ex ) {
                // // [TODO] log
                // ex.printStackTrace();
            } catch (Exception ex) {
                // [MEMO] allways delete from queue when fail
                // [TODO] log
                ex.printStackTrace();
                succeed = true; // [MEMO] to delete from queue
            }

            if (succeed) {
                queueClient.deleteMessage(item.getMessageId(), item.getPopReceipt());
            }
        }
    }

}
