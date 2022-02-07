import java.net.InetSocketAddress;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import org.eclipse.jetty.servlet.ErrorPageErrorHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class HttpService {
	private static final Logger logger_ = LoggerFactory.getLogger(HttpService.class);

	private static class StopHandler implements SignalHandler {
		@Override
		public void handle(Signal signal) {
			stop();
		}
	}

	public static void main (String [] args) {
		StopHandler handler = new StopHandler();
		Signal.handle(new Signal("TERM"), handler);
		Signal.handle(new Signal("INT"), handler);
		start(args);
	}

	private static Server server;

	private static void start (String [] args) {
		String host = args [0];
		int port = Integer.parseInt (args[1]);
		String warPath = args[2];
		logger_.info("starting HttpService host: " + host + " port: " + port + " warPath: " + warPath);

		InetSocketAddress ep = new InetSocketAddress(host,port);
		server = new Server(ep);
		WebAppContext webapp = new WebAppContext();
		webapp.setWar(warPath);

		ErrorPageErrorHandler errorHandler = new ErrorPageErrorHandler();
		errorHandler.addErrorPage(500, "/s500.html");
		errorHandler.addErrorPage(404, "/s404.html");
		webapp.setErrorHandler(errorHandler);

		server.setHandler(webapp);
		try {
			server.start();
		} catch (Exception ex) {
			logger_.error("failed to start", ex);
		}
	}

	private static void stop () {
		try {
			server.stop();
		} catch (Exception ex) {
			logger_.error("failed to stop", ex);
		}
	}

}
