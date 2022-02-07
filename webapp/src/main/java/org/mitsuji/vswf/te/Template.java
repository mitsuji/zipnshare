package org.mitsuji.vswf.te;

import java.util.ArrayList;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

import org.mitsuji.vswf.Util;


public class Template extends ArrayList<Template.TemplateElement> {

	//
	// handler types
	//

	public static interface PlaceHolderHandler {
		public void onPlaceHolder(String charset, String type, String title, OutputStream out) throws IOException;
	}


	//
	// element types
	//

	public static interface TemplateElement {}

	public static class StaticContent implements TemplateElement {
		public byte[] content;
		public StaticContent (byte[] content) {
			this.content = content;
		}
	}

	public static class PlaceHolder implements TemplateElement {
		public String type;
		public String title;
		public PlaceHolder (String type, String title) {
			this.type = type;
			this.title = title;
		}
	}


	//
	// object members
	//

	private String charset;
	private Template (String charset) {
		this.charset = charset;
	}
	public String getCharset() { return charset; }

	public void apply (PlaceHolderHandler handler, OutputStream out) throws IOException {
		for (TemplateElement tl : this) {
			if (tl instanceof StaticContent) {
				StaticContent sc = (StaticContent)tl;
				InputStream in = new ByteArrayInputStream(sc.content);
				Util.copy(in,out,256);
			}
			if (tl instanceof PlaceHolder) {
				PlaceHolder ph = (PlaceHolder)tl;
				handler.onPlaceHolder(charset,ph.type,ph.title,out);
			}
		}
	}

	//
	// static members
	//    
	private static enum ParseState {
		STATIC_CONTENT,
		DOLLAR,
		PLACE_HOLDER_TYPE,
		PLACE_HOLDER_TITLE
	}

	public static Template parse (InputStream in) throws IOException {
		return parse (in, "UTF-8"); // [TODO] get default charset ?
	}

	public static Template parse (InputStream in, String charset) throws IOException {
		//
		// ${type:title}
		//
		// '$' 0x24
		// '{' 0x7b
		// '}' 0x7d
		// ':' 0x3a

		Template result = new Template(charset);
		ParseState state = ParseState.STATIC_CONTENT;
		ByteArrayOutputStream out = new ByteArrayOutputStream ();
		String placeHolderType = null;

		int b;
		while ((b = in.read()) >= 0) {
			switch (state) {
			case STATIC_CONTENT:
				{
					switch (b) {
					case 0x24 : { // '$'
						state = ParseState.DOLLAR;
						break;
					}
					default : { // other
						out.write(b);
						break;
					}
					}
					break;
				}
			case DOLLAR:
				{
					switch (b) {
					case 0x7b : { // '{'
						if (out.size() > 0) {
							result.add(new StaticContent(out.toByteArray()));
						}
						out.reset();
						state = ParseState.PLACE_HOLDER_TYPE;
						break;
					}
					case 0x24 : { // '$'
						out.write(0x24); // '$'
						break;
					}
					default : { // other
						out.write(0x24); // '$'
						out.write(b);
						state = ParseState.STATIC_CONTENT;
						break;
					}
					}
					break;
				}
			case PLACE_HOLDER_TYPE:
				{
					switch (b) {
					case 0x3a : { // ':'
						if (out.size() > 0) {
							placeHolderType = new String(out.toByteArray(), charset);
						}
						out.reset();
						state = ParseState.PLACE_HOLDER_TITLE;
						break;
					}
					case 0x7d : { // '}'
						throw new IOException ("parce error: PlaceHolder name not specified.");
					}
					default : { // other
						out.write(b);
						break;
					}
					}
					break;
				}
			case PLACE_HOLDER_TITLE:
				{
					switch (b) {
					case 0x7d : { // '}'
						if (out.size() > 0) {
							result.add(new PlaceHolder(placeHolderType, new String(out.toByteArray(), charset)));
						}
						out.reset();
						state = ParseState.STATIC_CONTENT;
						break;
					}
					default : { // other
						out.write(b);
						break;
					}
					}
					break;
				}
			}
		}

		if (state == ParseState.PLACE_HOLDER_TYPE) {
			throw new IOException ("parce error: PlaceHolder type not completed.");
		}

		if (state == ParseState.PLACE_HOLDER_TITLE) {
			throw new IOException ("parce error: PlaceHolder name not completed.");
		}

		if (state == ParseState.DOLLAR) {
			out.write(0x24); // '$'
		}

		if (out.size() > 0) {
			result.add(new StaticContent(out.toByteArray()));
		}

		return result;
	}

}
