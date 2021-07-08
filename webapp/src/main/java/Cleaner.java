import type.BackgroundJob;

public class Cleaner implements Runnable {

    private long cleanIntervalSeconds;
    private BackgroundJob backgroundJob;
    public Cleaner (long cleanIntervalSeconds, BackgroundJob backgroundJob) {
	this.cleanIntervalSeconds = cleanIntervalSeconds;
	this.backgroundJob = backgroundJob;
    }

    public void run () {
	while (true) {
	    try {
		backgroundJob.clean();
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

