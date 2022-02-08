package org.mitsuji.vswf;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

import java.util.regex.Pattern;
import java.util.Random;
import java.security.SecureRandom;
import java.security.NoSuchAlgorithmException;

public class Util {

    public static void copy(InputStream in, OutputStream out, int buffLen) throws IOException {
        byte[] buff = new byte[buffLen];
        int len;
        while ((len = in.read(buff)) >= 0) {
            out.write(buff, 0, len);
        }
    }

    public static int getLineCount(InputStream in) throws IOException {
        int i = 0;
        int b;
        while ((b = in.read()) != -1) {
            if (b == 0xa) { // '\n'
                i++;
            }
        }
        return i;
    }

    public static boolean nullOrEmpty(String string) {
        return (string == null) || string.isEmpty();
    }

    public static boolean validTelNumber(String string) {
        return (string != null) && Pattern.matches("[[0-9]-]+", string);
    }

    public static boolean validEmailAddress(String string) {
        // return Pattern.matches("^([\\w]+)([\\w\\.-]+)@([\\w_-]+)\\.([\\w_\\.-]*)[a-z][a-z]$", string);
        return (string != null) && Pattern.matches("^([\\w\\.-]+)@([\\w_-]+)\\.([\\w_\\.-]*)[a-z][a-z]$", string); // simple
    }

    public static boolean validUrl(String string) {
        return (string != null) && Pattern.matches("http(s)?://([\\w-]+\\.)+[\\w-]+(/[\\w- ./?%&=]*)?$", string);
    }

    public static String genIntHexKey() throws NoSuchAlgorithmException {
        Random rnd = SecureRandom.getInstance("SHA1PRNG");
        int i = rnd.nextInt();
        return String.format("%08X", i);
    }

    public static String genAlphaNumericKey(int len) throws NoSuchAlgorithmException {
        char[] seeds = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
        StringBuffer buff = new StringBuffer();
        Random rnd = SecureRandom.getInstance("SHA1PRNG");
        for (int i = 0; i < len; i++) {
            int r = rnd.nextInt(seeds.length);
            buff.append(seeds[r]);
        }
        return buff.toString();
    }

}
