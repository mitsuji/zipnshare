import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.mitsuji.vswf.Util;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.core.sync.RequestBody;


import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;

public class Test {

    public static void main (String [] args) {
	try {
//	    upload (args);
//	    download (args);
//	    size (args);
//	    exists (args);
	    multipart (args);
	} catch (Exception ex) {
	    ex.printStackTrace();
	}
    }
    
    public static void upload (String [] args) {
	AwsBasicCredentials creds = AwsBasicCredentials.create(args[0], args[1]);
	S3Client s3 = S3Client.builder()
	    .region(Region.of("us-east-2"))
	    .credentialsProvider(StaticCredentialsProvider.create(creds))
	    .build();
	
	PutObjectRequest objectRequest = PutObjectRequest.builder()
	    .bucket("ns-zipnshare")
	    .key("some/dir/data1_key")
	    .build();
	
//	s3.putObject(objectRequest, RequestBody.fromString("data1_val"));
	InputStream in = new ByteArrayInputStream("ABCD1234".getBytes());
	s3.putObject(objectRequest, RequestBody.fromInputStream(in,8));
    }
    
    public static void download (String [] args) throws IOException {
	AwsBasicCredentials creds = AwsBasicCredentials.create(args[0], args[1]);
	S3Client s3 = S3Client.builder()
	    .region(Region.of("us-east-2"))
	    .credentialsProvider(StaticCredentialsProvider.create(creds))
	    .build();
	
	GetObjectRequest getObjectRequest = GetObjectRequest.builder()
	    .bucket("ns-zipnshare")
	    .key("some/dir1/multi1_key")
	    .build();

	InputStream in = s3.getObject(getObjectRequest);
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	Util.copy(in,out,1024*1024);
	System.out.println(new String(out.toByteArray()));
    }

    public static void size (String [] args) throws IOException {
	AwsBasicCredentials creds = AwsBasicCredentials.create(args[0], args[1]);
	S3Client s3 = S3Client.builder()
	    .region(Region.of("us-east-2"))
	    .credentialsProvider(StaticCredentialsProvider.create(creds))
	    .build();

	HeadObjectRequest headObjectRequest =
	    HeadObjectRequest.builder()
	    .bucket("ns-zipnshare")
	    .key("some/dir/data1_key")
	    .build();
	
	HeadObjectResponse headObjectResponse =
	    s3.headObject(headObjectRequest);
	
	Long contentLength = headObjectResponse.contentLength();
	
	System.out.println("size: " + contentLength);
    }

    public static void exists (String [] args) throws IOException {
	AwsBasicCredentials creds = AwsBasicCredentials.create(args[0], args[1]);
	S3Client s3 = S3Client.builder()
	    .region(Region.of("us-east-2"))
	    .credentialsProvider(StaticCredentialsProvider.create(creds))
	    .build();

	HeadObjectRequest headObjectRequest =
	    HeadObjectRequest.builder()
	    .bucket("ns-zipnshare")
	    .key("some/dir/data2_key")
	    .build();
	
	boolean exists = objectExists(s3,headObjectRequest);
	
	System.out.println("exists: " + exists);
    }


    public static boolean objectExists(S3Client s3, HeadObjectRequest headObjectRequest) {
	try {
	    HeadObjectResponse headResponse =
		s3.headObject(headObjectRequest);
	    return true;
	} catch (NoSuchKeyException e) {
	    return false;
	}
    }
    
    public static void multipart (String [] args) throws IOException {
	final String bucketName = "ns-zipnshare";
	final String key = "some/dir1/multi1_key";
	
	AwsBasicCredentials creds = AwsBasicCredentials.create(args[0], args[1]);
	S3Client s3 = S3Client.builder()
	    .region(Region.of("us-east-2"))
	    .credentialsProvider(StaticCredentialsProvider.create(creds))
	    .build();

	CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
	    .bucket(bucketName)
	    .key(key)
	    .build();
        CreateMultipartUploadResponse response = s3.createMultipartUpload(createMultipartUploadRequest);
	
        String uploadId = response.uploadId();
        System.out.println("uploadId: " + uploadId);

	ByteArrayOutputStream out = new ByteArrayOutputStream ();
	out.write("multi_val1".getBytes()); out.write(0xa);
	for (int i = 0; i < (5 * 1024 * 1024 -11); i++) {
	    out.write(0x20);
	}
	out.close(); // 5Mb

        UploadPartRequest uploadPartRequest1 = UploadPartRequest.builder()
	    .bucket(bucketName)
	    .key(key)
	    .uploadId(uploadId)
	    .partNumber(1).build();
	UploadPartResponse uploadPartResponse1 = s3.uploadPart(uploadPartRequest1, RequestBody.fromBytes(out.toByteArray()));
        String etag1 = uploadPartResponse1.eTag();
        System.out.println("etag1: " + etag1);
	

        UploadPartRequest uploadPartRequest2 = UploadPartRequest.builder()
	    .bucket(bucketName)
	    .key(key)
	    .uploadId(uploadId)
	    .partNumber(2).build();
	UploadPartResponse uploadPartResponse2 = s3.uploadPart(uploadPartRequest2, RequestBody.fromString("multi1_val2"));
        String etag2 = uploadPartResponse2.eTag();
        System.out.println("etag2: " + etag2);


        CompletedPart part1 = CompletedPart.builder()
	    .partNumber(1)
	    .eTag(etag1)
	    .build();
        CompletedPart part2 = CompletedPart.builder()
	    .partNumber(2)
	    .eTag(etag2)
	    .build();
        CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
	    .parts(part1, part2)
	    .build();

        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
	    .bucket(bucketName)
	    .key(key)
	    .uploadId(uploadId)
	    .multipartUpload(completedMultipartUpload)
	    .build();

        CompleteMultipartUploadResponse completeMultipartUploadResponse = s3.completeMultipartUpload(completeMultipartUploadRequest);
    }
    
    
}
