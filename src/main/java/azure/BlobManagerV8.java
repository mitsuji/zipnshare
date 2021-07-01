package azure;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;

import java.net.URISyntaxException;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.queue.*;

public class BlobManagerV8 {
	private CloudBlobClient blobClient;
	private String container;
	private String sessionKey;
	public BlobManagerV8 (CloudBlobClient blobClient, String container, String sessionKey) {
	    this.blobClient = blobClient;
	    this.container = container;
	    this.sessionKey = sessionKey;
	}

	private String getFileDataFilePath(int fileId) {
	    return sessionKey + "/" + Integer.toString(fileId);
	}

	private String getZipFileDataFilePath() {
	    return sessionKey + "/zip";
	}

	private CloudBlobContainer getBlobContainer() throws URISyntaxException, StorageException {
	    return blobClient.getContainerReference(container);
	}

	public void appendFileData(int fileId) throws URISyntaxException, StorageException {
	    // create file data file
	    CloudBlobContainer container = getBlobContainer();
	    CloudAppendBlob fileDataBlob = container.getAppendBlobReference(getFileDataFilePath(fileId));
	    fileDataBlob.createOrReplace();
	}
	public void upload (int fileId, InputStream in, long len) throws URISyntaxException, StorageException, IOException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudAppendBlob fileDataBlob = container.getAppendBlobReference(getFileDataFilePath(fileId));
	    fileDataBlob.append(in,len);
	}
	public void download (int fileId, OutputStream out) throws URISyntaxException, StorageException, IOException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudAppendBlob blob = container.getAppendBlobReference(getFileDataFilePath(fileId));
	    blob.download(out);
	}
	public long getFileSize(int fileId) throws URISyntaxException, StorageException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudBlob blob = container.getAppendBlobReference(getFileDataFilePath(fileId));
	    blob.downloadAttributes();
	    return blob.getProperties().getLength();
	}
	public void deleteAll() throws URISyntaxException, StorageException {
	    ByteArrayOutputStream out = new ByteArrayOutputStream ();
	    CloudBlobContainer container = getBlobContainer();
	    for(ListBlobItem item: container.listBlobs(sessionKey + "/")) {
		if (item instanceof CloudBlob) {
		    CloudBlob blob = (CloudBlob)item;
		    blob.delete();
		}
	    }
	}

	public void zipDownload (OutputStream out) throws URISyntaxException, StorageException, IOException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudAppendBlob blob = container.getAppendBlobReference(getZipFileDataFilePath());
	    blob.download(out);
	}
	public long getZipFileSize() throws URISyntaxException, StorageException {
	    CloudBlobContainer container = getBlobContainer();
	    CloudBlob blob = container.getAppendBlobReference(getZipFileDataFilePath());
	    blob.downloadAttributes();
	    return blob.getProperties().getLength();
	}

	//
	// https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.storage.blob.cloudblockblob.upload?view=azure-java-legacy
	// https://docs.microsoft.com/en-us/java/api/com.microsoft.azure.storage.blob.cloudblockblob.openoutputstream?view=azure-java-legacy

}
