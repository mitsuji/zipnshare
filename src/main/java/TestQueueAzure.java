import com.azure.storage.queue.*;
import com.azure.storage.queue.models.*;
    
public class TestQueueAzure {

    public static void main (String [] args) {
//	enqueue(args);
	dequeue(args);
    }

    private static void enqueue (String [] args) {
	QueueClient queueClient = new QueueClientBuilder()
	    .connectionString(args[0])
	    .queueName("zipnshare")
	    .buildClient();

	queueClient.sendMessage("fooo");
    }

    private static void dequeue (String [] args) {
	QueueClient queueClient = new QueueClientBuilder()
	    .connectionString(args[0])
	    .queueName("zipnshare")
	    .buildClient();
	
	QueueMessageItem item = queueClient.receiveMessage();
	if (item != null) {
	    System.out.println("messageBody: " + item.getBody().toString());
	    System.out.println("messageId: " + item.getMessageId());
	    System.out.println("popReceipt: " + item.getPopReceipt());
	    queueClient.deleteMessage(item.getMessageId(),item.getPopReceipt());
	} else {
	    System.out.println("queue empty");
	}
    }
}
