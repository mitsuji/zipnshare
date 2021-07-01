package aws;

import java.util.List;
import java.util.ArrayList;

import java.io.InputStream;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public class BlobManager {
	private S3Client s3Client;
	private String bucket;
	private String sessionKey;
	public BlobManager (S3Client s3Client, String bucket, String sessionKey) {
	    this.s3Client = s3Client;
	    this.bucket = bucket;
	    this.sessionKey = sessionKey;
	}

	private String getFileDataFilePath(int fileId) {
	    return sessionKey + "/" + Integer.toString(fileId);
	}

	private String getZipFileDataFilePath() {
	    return sessionKey + "/zip";
	}

	public String createMultiPart(int fileId) {
	    CreateMultipartUploadRequest multipartReq = CreateMultipartUploadRequest.builder()
		.bucket(bucket)
		.key(getFileDataFilePath(fileId))
		.build();
	    CreateMultipartUploadResponse multipartRes = s3Client.createMultipartUpload(multipartReq);
	    return multipartRes.uploadId();
	}
	public String uploadPart (int fileId, String uploadId, int partNumber, InputStream in, long len) {
	    UploadPartRequest req = UploadPartRequest.builder()
		.bucket(bucket)
		.key(getFileDataFilePath(fileId))
		.uploadId(uploadId)
		.partNumber(partNumber+1) // [MEMO] partNumber starts from 1
		.build();
	    UploadPartResponse res = s3Client.uploadPart(req, RequestBody.fromInputStream(in,len));
	    return res.eTag();
	}
	public void completeMultiPart(int fileId, String uploadId, List<String> etags) {
	    ArrayList<CompletedPart> parts = new ArrayList<CompletedPart>();
	    for (int i = 0; i < etags.size(); i++) {
		String etag = etags.get(i);
		CompletedPart part = CompletedPart.builder()
		    .partNumber(i+1) // [MEMO] partNumber starts from 1
		    .eTag(etag)
		    .build();
		parts.add(part);
	    }

	    CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
		.parts(parts)
		.build();
	    
	    CompleteMultipartUploadRequest req = CompleteMultipartUploadRequest.builder()
		.bucket(bucket)
		.key(getFileDataFilePath(fileId))
		.uploadId(uploadId)
		.multipartUpload(completedMultipartUpload)
		.build();
	    s3Client.completeMultipartUpload(req);
	}

	public InputStream getFileDataInputStream (int fileId) {
	    GetObjectRequest req = GetObjectRequest.builder()
		.bucket(bucket)
		.key(getFileDataFilePath(fileId))
		.build();
	    return s3Client.getObject(req);
	}
	public long getFileSize(int fileId) {
	    HeadObjectRequest req = HeadObjectRequest.builder()
		.bucket(bucket)
		.key(getFileDataFilePath(fileId))
		.build();
	    HeadObjectResponse res = s3Client.headObject(req);
	    return res.contentLength();
	}

	public void deleteAll() {
	    ListObjectsRequest listReq = ListObjectsRequest.builder()
		.bucket(bucket)
		.prefix(sessionKey + "/")
		.build();
	    ListObjectsResponse listRes = s3Client.listObjects(listReq);
	    for(S3Object obj : listRes.contents()) {
		DeleteObjectRequest req = DeleteObjectRequest.builder()
		    .bucket(bucket)
		    .key(obj.key())
		    .build();
		s3Client.deleteObject(req);
	    }
	    
	}

	public InputStream getZipFileDataInputStream () {
	    GetObjectRequest req = GetObjectRequest.builder()
		.bucket(bucket)
		.key(getZipFileDataFilePath())
		.build();
	    return s3Client.getObject(req);
	}
	public long getZipFileSize() {
	    HeadObjectRequest req = HeadObjectRequest.builder()
		.bucket(bucket)
		.key(getZipFileDataFilePath())
		.build();
	    HeadObjectResponse res = s3Client.headObject(req);
	    return res.contentLength();
	}

}


