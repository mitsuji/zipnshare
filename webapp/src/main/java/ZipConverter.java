import type.ZipQueueProcessor;

public class ZipConverter implements Runnable {

	private long zipConvertIntervalSeconds;
	private ZipQueueProcessor queueProcessor;
	public ZipConverter (long zipConvertIntervalSeconds, ZipQueueProcessor queueProcessor) {
		this.zipConvertIntervalSeconds = zipConvertIntervalSeconds;
		this.queueProcessor = queueProcessor;
	}

	public void run () {
		while (true) {
			try {
				queueProcessor.process();
				Thread.sleep (zipConvertIntervalSeconds * 1000);
			} catch (InterruptedException ex) {
				break;
			} catch (Exception ex) {
				// [TODO] log
				ex.printStackTrace();
			}
		}
	}

}

