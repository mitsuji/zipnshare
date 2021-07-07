import type.ZipQueueProcessor;

public class ZipConverter implements Runnable {

    private long cleanIntervalSeconds;
    private ZipQueueProcessor queueProcessor;
    public ZipConverter (long zipConvertIntervalSeconds, ZipQueueProcessor queueProcessor) {
	this.cleanIntervalSeconds = cleanIntervalSeconds;
	this.queueProcessor = queueProcessor;
    }

    public void run () {
	while (true) {
	    try {
		queueProcessor.process();
		Thread.sleep (cleanIntervalSeconds * 1000);
	    } catch (InterruptedException ex) {
		break;
	    } catch (Exception ex) {
		// [TODO] log
		ex.printStackTrace();
	    }
	}
    }

}

