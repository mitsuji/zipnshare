import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import javax.servlet.ServletException;
import javax.servlet.UnavailableException;

import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.server.ResourceService;

import java.util.Locale;
import java.util.ResourceBundle;
import java.util.MissingResourceException;

import java.util.Collection;
import java.util.List;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;

import org.mitsuji.vswf.te.Template;
import org.mitsuji.vswf.te.Multipart;
import org.mitsuji.vswf.te.HtmlPlaceHolderHandler;
import org.mitsuji.vswf.Router;
import org.mitsuji.vswf.Util;
import org.mitsuji.vswf.ZipWriter;

import java.util.regex.Matcher;
import type.FileListItem;
import type.DataStorage;

import aws.AwsS3Storage;
import azure.AzureBlobStorageV12;
import azure.AzureBlobStorageV8;

public class ZipnshareServlet extends DefaultServlet {
    private static final Logger logger_ = LoggerFactory.getLogger(ZipnshareServlet.class);

    private String warPath;
    private DataStorage dataStorage;

    public ZipnshareServlet () {
	super();
    }

    public ZipnshareServlet (ResourceService resourceServic) {
	super(resourceServic);
    }

    public void init () throws UnavailableException {
	super.init();
	warPath = getServletContext().getRealPath("");
	try {
	    ClassLoader cl = Thread.currentThread().getContextClassLoader();
	    ResourceBundle bundle = ResourceBundle.getBundle("zipnshare",Locale.getDefault(),cl);
	    int maxFileCount = Integer.valueOf(bundle.getString("zipnshare.maxFileCount"));
	    long maxFileSize = Long.valueOf(bundle.getString("zipnshare.maxFileSize"));
	    boolean useZipConverter = Boolean.valueOf(bundle.getString("zipnshare.useZipConverter"));
	    String storageType = bundle.getString("zipnshare.storageType");
	    if (storageType.equals("localFile")) {
		String uploadPath = bundle.getString("zipnshare.uploadPath");
		FileStorage fileStorage = new FileStorage(uploadPath, maxFileCount, maxFileSize, useZipConverter);
		fileStorage.init();
		dataStorage = fileStorage;
	    } else if (storageType.equals("azureBlobV8")) {
		String cosmosAccountEndpoint = bundle.getString("zipnshare.cosmosAccountEndpoint");
		String cosmosAccountKey = bundle.getString("zipnshare.cosmosAccountKey");
		String cosmosDatabase = bundle.getString("zipnshare.cosmosDatabase");
		String storageAccountCS = bundle.getString("zipnshare.storageAccountCS");
		String cloudBlobContainer = bundle.getString("zipnshare.cloudBlobContainer");
		String queueName = bundle.getString("zipnshare.queueName");
		AzureBlobStorageV8 azureBlobStorage = new AzureBlobStorageV8(cosmosAccountEndpoint,cosmosAccountKey,cosmosDatabase,storageAccountCS,cloudBlobContainer,queueName, maxFileCount,maxFileSize,useZipConverter);
		azureBlobStorage.init();
		dataStorage = azureBlobStorage;
	    } else if (storageType.equals("azureBlobV12")) {
		String cosmosAccountEndpoint = bundle.getString("zipnshare.cosmosAccountEndpoint");
		String cosmosAccountKey = bundle.getString("zipnshare.cosmosAccountKey");
		String cosmosDatabase = bundle.getString("zipnshare.cosmosDatabase");
		String storageAccountCS = bundle.getString("zipnshare.storageAccountCS");
		String blobServiceContainer = bundle.getString("zipnshare.blobServiceContainer");
		String queueName = bundle.getString("zipnshare.queueName");
		AzureBlobStorageV12 azureBlobStorage = new AzureBlobStorageV12(cosmosAccountEndpoint,cosmosAccountKey,cosmosDatabase,storageAccountCS,blobServiceContainer,queueName, maxFileCount,maxFileSize,useZipConverter);
		azureBlobStorage.init();
		dataStorage = azureBlobStorage;
	    } else if (storageType.equals("awsS3")) {
		String region = bundle.getString("zipnshare.awsRegion");
		String accessKeyId = bundle.getString("zipnshare.awsAccessKeyId");
		String secretAccessKey = bundle.getString("zipnshare.awsSecretAccessKey");
		String dynamoTable = bundle.getString("zipnshare.dynamoTable");
		String s3Bucket = bundle.getString("zipnshare.s3Bucket");
		String sqsUrl = bundle.getString("zipnshare.sqsUrl");
		String sqsGroupId = bundle.getString("zipnshare.sqsGroupId");
		AwsS3Storage awsS3Storage = new AwsS3Storage(region, accessKeyId, secretAccessKey, dynamoTable, s3Bucket, sqsUrl, sqsGroupId, maxFileCount, maxFileSize, useZipConverter);
		awsS3Storage.init();
		dataStorage = awsS3Storage;
	    } else {
		logger_.error("invalid storageType");
		// [MEMO] just treated as a 404 error
		throw new UnavailableException ("invalid storageType");
	    }
	} catch (DataStorage.DataStorageException ex) {
	    logger_.error("failed to init dataStorage.", ex);
	    // [MEMO] just treated as a 404 error
	    throw new UnavailableException ("failed to init dataStorage.");
	} catch (MissingResourceException ex) {
	    logger_.error("failed to init dataStorage.", ex);
	    // [MEMO] just treated as a 404 error
	    throw new UnavailableException ("failed to init dataStorage.");
	} catch (Exception ex) {
	    logger_.error("failed to init dataStorage.", ex);
	    // [MEMO] just treated as a 404 error
	    throw new UnavailableException ("failed to init dataStorage.");
	}
    }

    public void destroy () {
	dataStorage.destroy();
	super.destroy ();
    }

    private void renderHtml(String localPath, Template.PlaceHolderHandler values, HttpServletResponse res) throws IOException {
	InputStream tempIn = new FileInputStream(warPath + localPath);
	Template temp = Template.parse(tempIn);
	res.setContentType("text/html");
	temp.apply(values,res.getOutputStream());
    }

    private void renderTextError(HttpServletResponse res, String message, Throwable ex) throws IOException {
	res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	res.getWriter().print(message);
	logger_.error(message, ex);
    }

    private void renderTextWarn(HttpServletResponse res, String message, Throwable ex) throws IOException {
	res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	res.getWriter().print(message);
	logger_.warn(message, ex);
    }

    private void renderTextWarn(HttpServletResponse res, String message) throws IOException {
	res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	res.getWriter().print(message);
	logger_.warn(message);
    }

    private String getFileId(Collection<Part> parts) throws IOException {
	for (Part part : parts) {
	    if(part.getName().equals("fileId") && part.getContentType() == null) {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		Util.copy(part.getInputStream(),bout,32);
		return new String(bout.toByteArray(),"UTF-8");
	    }
	}
	return null;
    }
    private Part getFile(Collection<Part> parts) {
	for (Part part : parts) {
	    if(part.getName().equals("file") && part.getContentType() != null) {
		return part;
	    }
	}
	return null;
    }

    public static class SharePlaceHolderHandler extends HtmlPlaceHolderHandler {
	private String sessionKey;
	private boolean ziped;
	private List<FileListItem> files;
	private InputStream fileListTemplateStream;
	private InputStream zipFileListTemplateStream;
	public SharePlaceHolderHandler (String sessionKey, boolean ziped, List<FileListItem> files, String elementsPath) throws IOException {
	    super();
	    this.sessionKey = sessionKey;
	    this.ziped = ziped;
	    this.files = files;
	    Multipart multi = Multipart.parse(new FileInputStream(elementsPath));
	    fileListTemplateStream = multi.getInputStream("fileList");
	    zipFileListTemplateStream = multi.getInputStream("zipFileList");
	}
	public void onPlaceHolder(String charset, String type, String title, OutputStream out) throws IOException {
	    if (title.equals("fileList")) {
		Template tempFileList = Template.parse(fileListTemplateStream);
		int i = 0;
		for (FileListItem item : files) {
		    HtmlPlaceHolderHandler handler = new HtmlPlaceHolderHandler();
		    handler.put("sessionKey",sessionKey);
		    handler.put("fileId",Integer.toString(i));
		    handler.put("fileName",item.fileName);
		    handler.put("contentType",item.contentType);
		    tempFileList.apply(handler,out);
		    i++;
		}
	    } else if (title.equals("zipFileList")) {
		if (ziped) {
		    Template tempZipFileList = Template.parse(zipFileListTemplateStream);
		    HtmlPlaceHolderHandler handler = new HtmlPlaceHolderHandler();
		    handler.put("sessionKey",sessionKey);
		    tempZipFileList.apply(handler,out);
		}
	    } else {
		super.onPlaceHolder(charset,type,title,out);
	    }
	}
    }

    public void service(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {

	Router router = new Router(req);
	if (router.matches("POST","/upload")) {
	    res.setContentType("text/plain");
	    try {
		String sessionKey = dataStorage.createSession();
		res.getWriter().print(sessionKey);
	    } catch (DataStorage.DataStorageException ex) {
		renderTextError (res,"failed to createSession",ex);
	    } catch (Exception ex) {
		renderTextError (res,"failed to createSession",ex);
	    }
	} else if (router.matches("PUT","/upload/(\\w+)/set-metadata")) {
	    String sessionKey = router.getMatcher().group(1);
	    res.setContentType("text/plain");
	    try {
		if (dataStorage.hasLocked(sessionKey)) {
		    renderTextWarn (res,"session locked");
		} else {
		    String ownerKey = req.getParameter("ownerKey");
		    dataStorage.setOwnerKey(sessionKey,ownerKey);
		    res.getWriter().print(""); // [MEMO] SUCCESS
		}
	    } catch (DataStorage.NoSuchSessionException ex) {
		renderTextWarn (res,"no such session",ex);
	    } catch (DataStorage.DataStorageException ex) {
		renderTextError (res,"failed to setOwnerKey",ex);
	    } catch (Exception ex) {
		renderTextError (res,"failed to setOwnerKey",ex);
	    }
	} else if (router.matches("PUT","/upload/(\\w+)/begin-file")) {
	    String sessionKey = router.getMatcher().group(1);
	    res.setContentType("text/plain");
	    try {
		if (dataStorage.hasLocked(sessionKey)) {
		    renderTextWarn (res,"session locked");
		} else {
		    String fileName = req.getParameter("fileName");
		    String contentType = req.getParameter("contentType");
		    String fileId = dataStorage.createFileData(sessionKey,fileName,contentType);
		    res.getWriter().print(fileId);
		}
	    } catch (DataStorage.NoSuchSessionException ex) {
		renderTextWarn (res,"no such session",ex);
	    } catch (DataStorage.TooManyFilesException ex) {
		renderTextWarn (res,"too many files",ex);
	    } catch (DataStorage.DuplicatedFileNameException ex) {
		renderTextWarn (res,"duplicated file name",ex);
	    } catch (DataStorage.DataStorageException ex) {
		renderTextError (res,"failed to createFileData",ex);
	    } catch (Exception ex) {
		renderTextError (res,"failed to createFileData",ex);
	    }
	} else if (router.matches("PUT","/upload/(\\w+)/send-file")) {
	    String sessionKey = router.getMatcher().group(1);
	    res.setContentType("text/plain");
	    try {
		if (dataStorage.hasLocked(sessionKey)) {
		    renderTextWarn (res,"session locked");
		} else {
		    Collection<Part> parts = req.getParts();
		    String fileId = getFileId(parts);
		    Part file = getFile(parts);
		    dataStorage.upload(sessionKey,fileId,file.getInputStream(),file.getSize());
		    res.getWriter().print(""); // [MEMO] SUCCESS
		}
	    } catch (DataStorage.NoSuchSessionException ex) {
		renderTextWarn (res,"no such session",ex);
	    } catch (DataStorage.NoSuchFileDataException ex) {
		renderTextWarn (res,"no such file data",ex);
	    } catch (DataStorage.TooLargeFileException ex) {
		renderTextWarn (res,"too large file",ex);
	    } catch (DataStorage.DataStorageException ex) {
		renderTextError (res,"failed to upload",ex);
	    } catch (Exception ex) {
		renderTextError (res,"failed to upload",ex);
	    }
	} else if (router.matches("PUT","/upload/(\\w+)/end-file")) {
	    String sessionKey = router.getMatcher().group(1);
	    res.setContentType("text/plain");
	    try {
		if (dataStorage.hasLocked(sessionKey)) {
		    renderTextWarn (res,"session locked");
		} else {
		    String fileId = req.getParameter("fileId");
		    dataStorage.closeFileData (sessionKey,fileId);
		    res.getWriter().print(""); // [MEMO] SUCCESS
		}
	    } catch (DataStorage.NoSuchSessionException ex) {
		renderTextWarn (res,"no such session",ex);
	    } catch (DataStorage.NoSuchFileDataException ex) {
		renderTextWarn (res,"no such file data",ex);
	    } catch (DataStorage.DataStorageException ex) {
		renderTextError (res,"failed to closeFileData",ex);
	    } catch (Exception ex) {
		renderTextError (res,"failed to closeFileData",ex);
	    }
	} else if (router.matches("PUT","/upload/(\\w+)/end-session")) {
	    String sessionKey = router.getMatcher().group(1);
	    res.setContentType("text/plain");
	    try {
		if (dataStorage.hasLocked(sessionKey)) {
		    renderTextWarn (res,"session locked");
		} else {
		    dataStorage.lockSession(sessionKey);
		    res.getWriter().print(""); // [MEMO] SUCCESS
		}
	    } catch (DataStorage.NoSuchSessionException ex) {
		renderTextWarn (res,"no such session",ex);
	    } catch (DataStorage.DataStorageException ex) {
		renderTextError (res,"failed to lockSession",ex);
	    } catch (Exception ex) {
		renderTextError (res,"failed to lockSession",ex);
	    }
	} else if (router.matches("GET","/share_(\\w+).html")) {
	    String sessionKey = router.getMatcher().group(1);
	    try {
		if (!dataStorage.hasLocked(sessionKey)) {
		    // [TODO] 500
		    logger_.warn("session not locked");
		    throw new ServletException("session not locked");
		} else {
		    List<FileListItem> files = dataStorage.getFileList(sessionKey);
		    boolean ziped = dataStorage.hasZiped(sessionKey);
		    HtmlPlaceHolderHandler values = new SharePlaceHolderHandler(sessionKey,ziped,files,warPath + "/zipnshare/share_elements.html");
		    values.put("sessionKey",sessionKey);
		    renderHtml("/zipnshare/share.html",values,res);
		}
	    } catch (DataStorage.NoSuchSessionException ex) {
		// [TODO] 404
		logger_.warn("no such session",ex);
		throw new ServletException("no such session",ex);
	    } catch (DataStorage.DataStorageException ex) {
		// [TODO] 500
		logger_.error("failed to share_xxxx.html",ex);
		throw new ServletException("failed to share_xxxx.html",ex);
	    } catch (Exception ex) {
		// [TODO] 500
		logger_.error("failed to share_xxxx.html",ex);
		throw new ServletException("failed to share_xxxx.html",ex);
	    }
	} else if (router.matches("GET","/download/(\\w+)/zip")) {
	    Matcher m = router.getMatcher();
	    String sessionKey = m.group(1);
	    try {
		if (!dataStorage.hasLocked(sessionKey)) {
		    // [TODO] 500
		    logger_.warn("session not locked");
		    throw new ServletException("session not locked");
		} else {
		    if (!dataStorage.hasZiped(sessionKey)) {
			// [TODO] 500
			logger_.warn("session not ziped");
			throw new ServletException("session not ziped");
		    } else {
			long fileSize = dataStorage.getZipFileSize(sessionKey);
			String fileName = sessionKey + ".zip";
			res.setContentType("application/zip");
			res.setHeader("Content-Disposition", "attachment; filename=" + fileName + "; filename*=UTF-8''" + URLEncoder.encode(fileName,"UTF-8"));
			res.setHeader("Content-Length", Long.toString(fileSize));
			dataStorage.zipDownload(sessionKey,res.getOutputStream());
		    }
		}
	    } catch (DataStorage.NoSuchSessionException ex) {
		// [TODO] 404
		logger_.warn("no such session",ex);
		throw new ServletException("no such session",ex);
	    } catch (DataStorage.NoSuchFileDataException ex) {
		// [TODO] 404
		logger_.warn("no such file data",ex);
		throw new ServletException("no such file data",ex);
	    } catch (DataStorage.DataStorageException ex) {
		// [TODO] 500
		logger_.error("failed to download/xxxx/zip",ex);
		throw new ServletException("failed to download/xxxx/zip",ex);
	    } catch (Exception ex) {
		// [TODO] 500
		logger_.error("failed to download/xxxx/zip",ex);
		throw new ServletException("failed to download/xxxx/zip",ex);
	    }
	} else if (router.matches("GET","/download/(\\w+)/(\\d+)")) {
	    Matcher m = router.getMatcher();
	    String sessionKey = m.group(1);
	    String fileId = m.group(2);
	    try {
		if (!dataStorage.hasLocked(sessionKey)) {
		    // [TODO] 500
		    logger_.warn("session not locked");
		    throw new ServletException("session not locked");
		} else {
		    long fileSize = dataStorage.getFileSize(sessionKey,fileId);
		    FileListItem fileInfo = dataStorage.getFileInfo(sessionKey,fileId);
		    String contentType = fileInfo.contentType;
		    String fileName = fileInfo.fileName;
		    if (contentType != null && !contentType.isEmpty()) {
			res.setContentType(contentType);
		    } else {
			res.setContentType("application/octet-stream");
		    }
		    res.setHeader("Content-Disposition", "attachment; filename=" + fileName + "; filename*=UTF-8''" + URLEncoder.encode(fileName,"UTF-8"));
		    res.setHeader("Content-Length", Long.toString(fileSize));
		    dataStorage.download(sessionKey,fileId,res.getOutputStream());
		}
	    } catch (DataStorage.NoSuchSessionException ex) {
		// [TODO] 404
		logger_.warn("no such session",ex);
		throw new ServletException("no such session",ex);
	    } catch (DataStorage.NoSuchFileDataException ex) {
		// [TODO] 404
		logger_.warn("no such file data",ex);
		throw new ServletException("no such file data",ex);
	    } catch (DataStorage.DataStorageException ex) {
		// [TODO] 500
		logger_.error("failed to download/xxxx/n",ex);
		throw new ServletException("failed to download/xxxx/n",ex);
	    } catch (Exception ex) {
		// [TODO] 500
		logger_.error("failed to download/xxxx/n",ex);
		throw new ServletException("failed to download/xxxx/n",ex);
	    }
	} else if (router.matches("GET","/delete_(\\w+).html")) {
	    String sessionKey = router.getMatcher().group(1);
	    try {
		if (!dataStorage.hasLocked(sessionKey)) {
		    // [TODO] 500
		    logger_.warn("session not locked");
		    throw new ServletException("session not locked");
		} else {
		    HtmlPlaceHolderHandler values = new HtmlPlaceHolderHandler();
		    values.put("sessionKey",sessionKey);
		    renderHtml("/zipnshare/delete.html",values,res);
		}
	    } catch (DataStorage.NoSuchSessionException ex) {
		// [TODO] 404
		logger_.warn("no such session",ex);
		throw new ServletException("no such session",ex);
	    } catch (DataStorage.DataStorageException ex) {
		// [TODO] 500
		logger_.error("failed to delete_xxxx.html",ex);
		throw new ServletException("failed to delete_xxxx.html",ex);
	    } catch (Exception ex) {
		// [TODO] 500
		logger_.error("failed to delete_xxxx.html",ex);
		throw new ServletException("failed to delete_xxxx.html",ex);
	    }

	} else if (router.matches("POST","/delete/(\\w+)")) {
	    String sessionKey = router.getMatcher().group(1);
	    res.setContentType("text/plain");
	    try {
		if (!dataStorage.hasLocked(sessionKey)) {
		    renderTextWarn (res,"session not locked");
		} else {
		    String ownerKey = req.getParameter("ownerKey");
		    if (!dataStorage.matchOwnerKey(sessionKey, ownerKey)) {
			renderTextWarn (res,"invalid owner key");
		    } else {
			dataStorage.deleteSession(sessionKey);
			res.getWriter().print(""); // [MEMO] SUCCESS
		    }
		}
	    } catch (DataStorage.NoSuchSessionException ex) {
		renderTextWarn (res,"no such session",ex);
	    } catch (DataStorage.DataStorageException ex) {
		renderTextError (res,"failed to deleteSession",ex);
	    } catch (Exception ex) {
		renderTextError (res,"failed to deleteSession",ex);
	    }
	} else {
	    super.service(req, res); // serve file content
	}

    }

}
