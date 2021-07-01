package azure;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.azure.storage.blob.*;
import com.azure.storage.blob.specialized.*;
import com.azure.storage.blob.models.*;

import org.mitsuji.vswf.Util;

public class BlobManagerV12 {
	private BlobServiceClient blobServiceClient;
	private String blobServiceContainer;
	private String sessionKey;
	public BlobManagerV12 (BlobServiceClient blobServiceClient, String blobServiceContainer, String sessionKey) {
	    this.blobServiceClient = blobServiceClient;
	    this.blobServiceContainer = blobServiceContainer;
	    this.sessionKey = sessionKey;
	}

	private String getFileDataBlobPath(int fileId) {
	    return sessionKey + "/" + Integer.toString(fileId);
	}

	private String getZipFileDataBlobPath() {
	    return sessionKey + "/zip";
	}

	private BlobContainerClient getBlobContainerClient() {
	    return blobServiceClient.getBlobContainerClient(blobServiceContainer);
	}

	public void appendFileData(int fileId) {
	    // create file data file
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient fileDataBlobClient = containerClient.getBlobClient(getFileDataBlobPath(fileId));
	    AppendBlobClient fileDataAppendBlobClient = fileDataBlobClient.getAppendBlobClient();
	    fileDataAppendBlobClient.create();
	}
	public void upload (int fileId, InputStream in, long len) throws IOException {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileDataBlobPath(fileId));
	    AppendBlobClient appendBlobClient = blobClient.getAppendBlobClient();
//	    appendBlobClient.appendBlock(in,len); // unsusable due to maxLimit 4194304 byte error
	    OutputStream out = appendBlobClient.getBlobOutputStream();
	    Util.copy(in,out,1024 * 1024);
	}
	public void download (int fileId, OutputStream out) {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileDataBlobPath(fileId));
	    blobClient.download(out);
	}

	public long getFileSize(int fileId) {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileDataBlobPath(fileId));
	    return blobClient.getProperties().getBlobSize();
	}

	public void deleteAll() {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    for(BlobItem item: containerClient.listBlobsByHierarchy(sessionKey + "/")) {
		BlobClient blobClient = containerClient.getBlobClient(item.getName());
		blobClient.delete();
	    }
	}

	public void zipDownload (OutputStream out) {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getZipFileDataBlobPath());
	    blobClient.download(out);
	}

	public long getZipFileSize() {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getZipFileDataBlobPath());
	    return blobClient.getProperties().getBlobSize();
	}

	// https://docs.microsoft.com/en-us/java/api/com.azure.storage.blob.specialized.blockblobclient.getbloboutputstream?view=azure-java-stable
	// https://docs.microsoft.com/en-us/java/api/com.azure.storage.blob.specialized.blockblobclient.upload?view=azure-java-stable
	public OutputStream getZipOutputStream () {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getZipFileDataBlobPath());
	    BlockBlobClient blockBlobClient = blobClient.getBlockBlobClient();
	    return blockBlobClient.getBlobOutputStream();
	}

	public InputStream getFileInputStream (int fileId) {
	    BlobContainerClient containerClient = getBlobContainerClient();
	    BlobClient blobClient = containerClient.getBlobClient(getFileDataBlobPath(fileId));
	    return blobClient.openInputStream();
	}

}
