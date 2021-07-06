package type;


public interface BackgroundJob {
	public static class BackgroundJobException extends Exception {
	    public BackgroundJobException (String message) {
		super(message);
	    }
	    public BackgroundJobException (String message, Throwable cause) {
		super(message, cause);
	    }
	};
	public static class NoSuchSessionException extends BackgroundJobException {
	    public NoSuchSessionException (String message) {
		super(message);
	    }
	};

	public void clean() throws BackgroundJobException;
	public void zipConvert(String sessonKey) throws BackgroundJobException;

}
