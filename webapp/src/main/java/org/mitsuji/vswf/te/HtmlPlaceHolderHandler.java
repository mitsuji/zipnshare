package org.mitsuji.vswf.te;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

import java.io.ByteArrayInputStream;
import java.util.HashMap;

import org.mitsuji.vswf.Util;

public class HtmlPlaceHolderHandler extends HashMap<String,String> implements Template.PlaceHolderHandler {

    private InputStream getInputStream(String charset, String title) throws IOException {
	String value;
	if (containsKey(title)) {
	    value = get(title);
	    if (value == null) {
		value = "";
	    }
	} else {
	    value = "";
	}
	ByteArrayInputStream in = new ByteArrayInputStream(value.getBytes(charset));
	return in;
    }
    
    public void onPlaceHolder(String charset, String type, String title, OutputStream out) throws IOException {
	InputStream in = getInputStream(charset,title);
	if (type.equals("html")) {
	    escapeHtml(in,out);
	} else {
	    Util.copy(in,out,256);
	}
    }

    // [MEMO] public for test
    public static void escapeHtml(InputStream in, OutputStream out) throws IOException {
	final String C_7BIT_CHARSET = "UTF-8";
	
	int c;
	while((c = in.read()) != -1) {
	    switch (c) {
	    case '"': {
		out.write("&quot;".getBytes(C_7BIT_CHARSET));
		break;
	    }
	    case '\'': {
		out.write("&#39;".getBytes(C_7BIT_CHARSET));
		break;
	    }
	    case '<': {
		out.write("&lt;".getBytes(C_7BIT_CHARSET));
		break;
	    }
	    case '>': {
		out.write("&gt;".getBytes(C_7BIT_CHARSET));
		break;
	    }
	    case '&': {
		out.write("&amp;".getBytes(C_7BIT_CHARSET));
		break;
	    }
	    default : {
		out.write(c);
		break;
	    }
	    }
	}
    }
    
}    
