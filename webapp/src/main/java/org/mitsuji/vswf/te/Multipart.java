package org.mitsuji.vswf.te;

import java.util.HashMap;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

import org.mitsuji.vswf.Util;

public class Multipart extends HashMap<String, byte[]> {

    //
    // object members
    //

    public InputStream getInputStream(String title) {
        if (containsKey(title)) {
            return new ByteArrayInputStream(get(title));
        } else {
            return null;
        }
    }

    //
    // static members
    //

    private static enum ParseState {
        STATIC_CONTENT, NUMBER, TITLE
    }

    public static Multipart parse(InputStream in) throws IOException {
        return parse(in, "UTF-8"); // [TODO] get default charset ?
    }

    public static Multipart parse(InputStream in, String charset) throws IOException {
        //
        // #{title}
        //
        // '#' 0x23
        // '{' 0x7b
        // '}' 0x7d

        int b;
        b = in.read();
        if (b != 0x23) {
            throw new IOException("parce error: Multipart must start with '#'.");
        }
        b = in.read();
        if (b != 0x7b) {
            throw new IOException("parce error: Multipart must start with \"#{\".");
        }

        Multipart result = new Multipart();
        ParseState state = ParseState.TITLE;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] title = null;

        while ((b = in.read()) >= 0) {
            switch (state) {
            case TITLE: {
                switch (b) {
                case 0x7d: { // '}'
                    if (out.size() > 0) {
                        title = out.toByteArray();
                    }
                    out.reset();
                    state = ParseState.STATIC_CONTENT;
                    break;
                }
                default: { // other
                    out.write(b);
                    break;
                }
                }
                break;
            }
            case NUMBER: {
                switch (b) {
                case 0x7b: { // '{'
                    if (out.size() > 0) {
                        result.put(new String(title, charset), out.toByteArray());
                    }
                    out.reset();
                    state = ParseState.TITLE;
                    break;
                }
                case 0x23: { // '#'
                    out.write(0x23); // '#'
                    break;
                }
                default: { // other
                    out.write(0x23); // '#'
                    out.write(b);
                    state = ParseState.STATIC_CONTENT;
                    break;
                }
                }
                break;
            }
            case STATIC_CONTENT: {
                switch (b) {
                case 0x23: { // '#'
                    state = ParseState.NUMBER;
                    break;
                }
                default: { // other
                    out.write(b);
                    break;
                }
                }
                break;
            }
            }
        }

        if (state == ParseState.TITLE) {
            throw new IOException("parce error: title name not completed.");
        }

        if (state == ParseState.NUMBER) {
            out.write(0x23); // '#'
        }

        if (out.size() > 0) {
            result.put(new String(title, charset), out.toByteArray());
        }

        return result;
    }

}
