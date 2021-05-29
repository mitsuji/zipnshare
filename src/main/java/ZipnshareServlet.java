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

import java.util.Properties;
import java.util.Collection;
import java.util.List;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;

import org.mitsuji.vswf.te.Template;
import org.mitsuji.vswf.te.Multipart;
import org.mitsuji.vswf.te.HtmlPlaceHolderHandler;
import org.mitsuji.vswf.Router;
import org.mitsuji.vswf.Util;
import org.mitsuji.vswf.ZipWriter;

import java.util.regex.Matcher;


public class ZipnshareServlet extends DefaultServlet {
    private static final Logger logger_ = LoggerFactory.getLogger(ZipnshareServlet.class);

    public static interface DataStorage {
	public static class DataStorageException extends Exception {
	    public DataStorageException (String message) {
		super(message);
	    }
	    public DataStorageException (String message, Throwable cause) {
		super(message, cause);
	    }
	};
	public static class NoSuchSessionException extends DataStorageException {
	    public NoSuchSessionException (String message) {
		super(message);
	    }
	};
	public static class NoSuchFileDataException extends DataStorageException {
	    public NoSuchFileDataException (String message) {
		super(message);
	    }
	};
	public static class TooManyFilesException extends DataStorageException {
	    public TooManyFilesException (String message) {
		super(message);
	    }
	};
	public static class DuplicatedFileNameException extends DataStorageException {
	    public DuplicatedFileNameException (String message) {
		super(message);
	    }
	};
	public static class TooLargeFileException extends DataStorageException {
	    public TooLargeFileException (String message) {
		super(message);
	    }
	};

	public static class FileListItem {
	    public String fileName;
	    public String contentType;
	    public FileListItem(String fileName, String contentType) {
		this.fileName = fileName;
		this.contentType = contentType;
	    }
	}

	public String createSession () throws DataStorageException;
	public void setOwnerKey (String sessionKey, String ownerKey) throws DataStorageException;
	public String createFileData (String sessionKey, String fileName, String contentType) throws DataStorageException;
	public void upload (String sessionKey, String fileId, InputStream in, long len) throws DataStorageException;
	public void closeFileData (String sessionKey, String fileId) throws DataStorageException;
	public void lockSession (String sessionKey) throws DataStorageException;

	public boolean hasLocked (String sessionKey) throws DataStorageException;
	public long getFileSize (String sessionKey, String fileId) throws DataStorageException;
	public List<FileListItem> getFileList (String sessionKey) throws DataStorageException;
	public FileListItem getFileInfo (String sessionKey, String fileId) throws DataStorageException;
	public void download (String sessionKey, String fileId, OutputStream out) throws DataStorageException;

	public boolean matchOwnerKey (String sessionKey, String ownerKey) throws DataStorageException;
	public void deleteSession (String sessionKey) throws DataStorageException;
    }

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
	    Properties prop = new Properties();
	    String configPath = warPath + "/WEB-INF/config/config.properties";
	    prop.load(new InputStreamReader(new FileInputStream(configPath),"UTF-8"));
	    int maxFileCount = Integer.valueOf(prop.getProperty("zipnshare.maxFileCount"));
	    long maxFileSize = Long.valueOf(prop.getProperty("zipnshare.maxFileSize"));
	    String storageType = prop.getProperty("zipnshare.storageType");
	    if (storageType.equals("localFile")) {
		String uploadPath = prop.getProperty("zipnshare.uploadPath");
		dataStorage = new FileStorage(uploadPath, maxFileCount, maxFileSize);
	    } else if (storageType.equals("azureBlobV8")) {
		String azureBlobCS = prop.getProperty("zipnshare.azureBlobCS");
		String azureBlobContainer = prop.getProperty("zipnshare.azureBlobContainer");
		dataStorage = new AzureBlobStorageV8(azureBlobCS, azureBlobContainer, maxFileCount, maxFileSize);
	    } else if (storageType.equals("azureBlobV12")) {
		String azureBlobCS = prop.getProperty("zipnshare.azureBlobCS");
		String azureBlobContainer = prop.getProperty("zipnshare.azureBlobContainer");
		dataStorage = new AzureBlobStorageV12(azureBlobCS, azureBlobContainer, maxFileCount, maxFileSize);
	    } else if (storageType.equals("awsS3")) {
		String region = prop.getProperty("zipnshare.awsRegion");
		String accessKeyId = prop.getProperty("zipnshare.awsAccessKeyId");
		String secretAccessKey = prop.getProperty("zipnshare.awsSecretAccessKey");
		String bucketName = prop.getProperty("zipnshare.awsBucketName");
		dataStorage = new AwsS3Storage(region, accessKeyId, secretAccessKey, bucketName, maxFileCount, maxFileSize);
	    } else {
		// [MEMO] just treated as a 404 error
		throw new UnavailableException ("invalid storageType");
	    }
	} catch (IOException ex) {
	    // [MEMO] just treated as a 404 error
	    throw new UnavailableException ("failed to load config.properties");
	}
    }

    private void renderHtml(String localPath, Template.PlaceHolderHandler values, HttpServletResponse res) throws IOException {
	InputStream tempIn = new FileInputStream(warPath + localPath);
	Template temp = Template.parse(tempIn);
	res.setContentType("text/html");
	temp.apply(values,res.getOutputStream());
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
    private Part getFile(Collection<Part> parts) throws IOException {
	for (Part part : parts) {
	    if(part.getName().equals("file") && part.getContentType() != null) {
		return part;
	    }
	}
	return null;
    }

    public static class SharePlaceHolderHandler extends HtmlPlaceHolderHandler {
	private String sessionKey;
	private List<DataStorage.FileListItem> files;
	private InputStream fileListTemplateStream;
	public SharePlaceHolderHandler (String sessionKey, List<DataStorage.FileListItem> files, String elementsPath) throws IOException {
	    super();
	    this.sessionKey = sessionKey;
	    this.files = files;
	    Multipart multi = Multipart.parse(new FileInputStream(elementsPath));
	    fileListTemplateStream = multi.getInputStream("fileList");
	}
	public void onPlaceHolder(String charset, String type, String title, OutputStream out) throws IOException {
	    if (title.equals("fileList")) {
		Template tempFileList = Template.parse(fileListTemplateStream);
		int i = 0;
		for (DataStorage.FileListItem item : files) {
		    HtmlPlaceHolderHandler handler = new HtmlPlaceHolderHandler();
		    handler.put("sessionKey",sessionKey);
		    handler.put("fileId",Integer.toString(i));
		    handler.put("fileName",item.fileName);
		    handler.put("contentType",item.contentType);
		    tempFileList.apply(handler,out);
		    i++;
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
		res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		res.getWriter().print(ex.getMessage());
	    }
	} else if (router.matches("PUT","/upload/(\\w+)/set-metadata")) {
	    String sessionKey = router.getMatcher().group(1);
	    String ownerKey = req.getParameter("ownerKey");
	    res.setContentType("text/plain");
	    try {
		if (dataStorage.hasLocked(sessionKey)) {
		    res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		    res.getWriter().print("failed to set-metadata: session locked");
		} else {
		    dataStorage.setOwnerKey(sessionKey,ownerKey);
		    res.getWriter().print("");
		}
	    } catch (DataStorage.DataStorageException ex) {
		res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		res.getWriter().print(ex.getMessage());
	    }
	} else if (router.matches("PUT","/upload/(\\w+)/begin-file")) {
	    String sessionKey = router.getMatcher().group(1);
	    String fileName = req.getParameter("fileName");
	    String contentType = req.getParameter("contentType");
	    res.setContentType("text/plain");
	    try {
		if (dataStorage.hasLocked(sessionKey)) {
		    res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		    res.getWriter().print("failed to begin-file: session locked");
		} else {
		    String fileId = dataStorage.createFileData(sessionKey,fileName,contentType);
		    res.getWriter().print(fileId);
		}
	    } catch (DataStorage.TooManyFilesException ex) {
		res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		res.getWriter().print(ex.getMessage());
	    } catch (DataStorage.DuplicatedFileNameException ex) {
		res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		res.getWriter().print(ex.getMessage());
	    } catch (DataStorage.DataStorageException ex) {
		res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		res.getWriter().print(ex.getMessage());
	    }
	} else if (router.matches("PUT","/upload/(\\w+)/send-file")) {
	    String sessionKey = router.getMatcher().group(1);
	    Collection<Part> parts = req.getParts();
	    String fileId = getFileId(parts);
	    Part file = getFile(parts);
	    res.setContentType("text/plain");
	    try {
		if (dataStorage.hasLocked(sessionKey)) {
		    res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		    res.getWriter().print("failed to send-file: session locked");
		} else {
		    dataStorage.upload(sessionKey,fileId,file.getInputStream(),file.getSize());
		    res.getWriter().print("");
		}
	    } catch (DataStorage.TooLargeFileException ex) {
		res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		res.getWriter().print(ex.getMessage());
	    } catch (DataStorage.DataStorageException ex) {
		res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		res.getWriter().print(ex.getMessage());
	    }
	} else if (router.matches("PUT","/upload/(\\w+)/end-file")) {
	    String sessionKey = router.getMatcher().group(1);
	    String fileId = req.getParameter("fileId");
	    res.setContentType("text/plain");
	    try {
		if (dataStorage.hasLocked(sessionKey)) {
		    res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		    res.getWriter().print("failed to end-file: session locked");
		} else {
		    dataStorage.closeFileData (sessionKey,fileId);
		}
	    } catch (DataStorage.DataStorageException ex) {
		res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		res.getWriter().print(ex.getMessage());
	    }
	} else if (router.matches("PUT","/upload/(\\w+)/end-session")) {
	    String sessionKey = router.getMatcher().group(1);
	    res.setContentType("text/plain");
	    try {
		if (dataStorage.hasLocked(sessionKey)) {
		    res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		    res.getWriter().print("failed to end-session: session locked");
		} else {
		    dataStorage.lockSession(sessionKey);
		    res.getWriter().print("");
		}
	    } catch (DataStorage.DataStorageException ex) {
		res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		res.getWriter().print(ex.getMessage());
	    }
	} else if (router.matches("GET","/share_(\\w+).html")) {
	    String sessionKey = router.getMatcher().group(1);
	    try {
		if (!dataStorage.hasLocked(sessionKey)) {
		    throw new ServletException("session not locked");
		} else {
		    List<DataStorage.FileListItem> files = dataStorage.getFileList(sessionKey);
		    HtmlPlaceHolderHandler values = new SharePlaceHolderHandler(sessionKey,files,warPath + "/zipnshare/share_elements.html");
		    values.put("sessionKey",sessionKey);
		    renderHtml("/zipnshare/share.html",values,res);
		}
	    } catch (DataStorage.NoSuchSessionException ex) {
		// [TODO] 404
		throw new ServletException(ex.getMessage());
	    } catch (DataStorage.DataStorageException ex) {
		throw new ServletException(ex.getMessage());
	    }
	} else if (router.matches("GET","/download/(\\w+)/(\\d+)")) {
	    Matcher m = router.getMatcher();
	    String sessionKey = m.group(1);
	    String fileId = m.group(2);
	    try {
		if (!dataStorage.hasLocked(sessionKey)) {
		    throw new ServletException("session not locked");
		}
		long fileSize = dataStorage.getFileSize(sessionKey,fileId);
		DataStorage.FileListItem fileInfo = dataStorage.getFileInfo(sessionKey,fileId);
		String contentType = fileInfo.contentType;
		String fileName = fileInfo.fileName;
		if (contentType != null && !contentType.isEmpty()) {
		    res.setContentType(contentType);
		} else {
		    res.setContentType("application/octet-stream");
		}
		res.setHeader("Content-Disposition", "attachment; filename=" + fileName);
		res.setHeader("Content-Length", Long.toString(fileSize));
		dataStorage.download(sessionKey,fileId,res.getOutputStream());
	    } catch (DataStorage.NoSuchSessionException ex) {
		// [TODO] 404
		throw new ServletException(ex.getMessage());
	    } catch (DataStorage.NoSuchFileDataException ex) {
		// [TODO] 404
		throw new ServletException(ex.getMessage());
	    } catch (DataStorage.DataStorageException ex) {
		throw new ServletException(ex.getMessage());
	    }
	} else if (router.matches("GET","/delete_(\\w+).html")) {
	    String sessionKey = router.getMatcher().group(1);
	    try {
		if (!dataStorage.hasLocked(sessionKey)) {
		    throw new ServletException("session not locked");
		} else {
		    HtmlPlaceHolderHandler values = new HtmlPlaceHolderHandler();
		    values.put("sessionKey",sessionKey);
		    renderHtml("/zipnshare/delete.html",values,res);
		}
	    } catch (DataStorage.NoSuchSessionException ex) {
		// [TODO] 404
		throw new ServletException(ex.getMessage());
	    } catch (DataStorage.DataStorageException ex) {
		throw new ServletException(ex.getMessage());
	    }

	} else if (router.matches("POST","/delete/(\\w+)")) {
	    String sessionKey = router.getMatcher().group(1);
	    String ownerKey = req.getParameter("ownerKey");
	    res.setContentType("text/plain");
	    try {
		if (!dataStorage.hasLocked(sessionKey)) {
		    res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		    res.getWriter().print("session not locked");
		} else {
		    if (!dataStorage.matchOwnerKey(sessionKey, ownerKey)) {
			res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			res.getWriter().print("invalid owner key");
		    } else {
			dataStorage.deleteSession(sessionKey);
			res.getWriter().print("");
		    }
		}
	    } catch (DataStorage.NoSuchSessionException ex) {
		// [TODO] 404
		res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		res.getWriter().print(ex.getMessage());
	    } catch (DataStorage.DataStorageException ex) {
		res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		res.getWriter().print(ex.getMessage());
	    }
	} else {
	    super.service(req, res); // serve file content
	}

    }

}
