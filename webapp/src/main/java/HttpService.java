import java.net.InetSocketAddress;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import org.eclipse.jetty.servlet.ErrorPageErrorHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class HttpService implements SignalHandler {
	private static final Logger logger_ = LoggerFactory.getLogger(HttpService.class);

	public static void main (String [] args) {
		HttpService service = new HttpService(args);
		Signal.handle(new Signal("TERM"), service);
		Signal.handle(new Signal("INT"), service);
		service.start();
	}

	private Server server;

	private HttpService (String [] args) {
		String host = args [0];
		int port = Integer.parseInt (args[1]);
		String warPath = args[2];
		logger_.info("starting HttpService host: " + host + " port: " + port + " warPath: " + warPath);

		InetSocketAddress ep = new InetSocketAddress(host,port);
		server = new Server(ep);
		WebAppContext webapp = new WebAppContext();
		webapp.setWar(warPath);

		ErrorPageErrorHandler errorHandler = new ErrorPageErrorHandler();
		errorHandler.addErrorPage(500, "/500.html");
		errorHandler.addErrorPage(404, "/404.html");
		webapp.setErrorHandler(errorHandler);

		server.setHandler(webapp);
	}

	@Override
	public void handle(Signal signal) {
		stop();
	}

	private void start () {
		try {
			server.start();
		} catch (Exception ex) {
			logger_.error("failed to start", ex);
		}
	}

	private void stop () {
		try {
			server.stop();
		} catch (Exception ex) {
			logger_.error("failed to stop", ex);
		}
	}

}
