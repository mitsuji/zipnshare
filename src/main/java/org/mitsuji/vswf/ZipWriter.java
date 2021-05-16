package org.mitsuji.vswf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import net.lingala.zip4j.io.outputstream.ZipOutputStream;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.EncryptionMethod;

public class ZipWriter {
    private ZipOutputStream zout;
    private ZipParameters zparam;

    public ZipWriter (OutputStream out, String password) throws IOException {
	zout = new ZipOutputStream (out, password.toCharArray());
	zparam = new ZipParameters();
	zparam.setEncryptFiles(true);
	zparam.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD);
    }

    public ZipWriter (OutputStream out) throws IOException {
	zout = new ZipOutputStream (out);
	zparam = new ZipParameters();
    }

    public void append(String fileNameInZip, InputStream in) throws IOException {
	ZipParameters zparam0 = new ZipParameters(zparam);
	zparam0.setFileNameInZip(fileNameInZip);
	zout.putNextEntry(zparam0);
	Util.copy(in,zout,256);
	zout.closeEntry();
    }
    
    public void close() throws IOException {
	zout.close();
    }

}
