package org.mitsuji.vswf;

import javax.servlet.http.HttpServletRequest;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Router {
    private HttpServletRequest req;
    private Matcher matcher;
    public Router (HttpServletRequest req) {
	this.req = req;
    }
    public Matcher getMatcher() {return matcher;}
    public boolean matches (String method, String pathPattern) {
	matcher = null;
	if (!req.getMethod().equals(method)) {
	    return false;
	}
	Pattern pattern = Pattern.compile(pathPattern);
	matcher = pattern.matcher(req.getPathInfo());
	return matcher.matches();
    }
}
    
