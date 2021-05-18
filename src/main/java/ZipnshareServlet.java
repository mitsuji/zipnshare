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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;

import org.mitsuji.vswf.te.Template;
import org.mitsuji.vswf.te.HtmlPlaceHolderHandler;
import org.mitsuji.vswf.Router;
import org.mitsuji.vswf.Util;
import org.mitsuji.vswf.ZipWriter;

import java.util.regex.Matcher;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;


public class ZipnshareServlet extends DefaultServlet {
  private static final Logger logger_ = LoggerFactory.getLogger(ZipnshareServlet.class);

  public static interface DataStorage {
    public OutputStream getUploadStream (String filename) throws IOException;
    public void download (String filename, OutputStream out) throws IOException;
    public boolean exists (String filename) throws IOException;
    public void deleteExpired () throws IOException;
  }

  private String warPath;
  private DataStorage dataStorage;

  public ZipnshareServlet () throws IOException {
      super();
  }
    
  public ZipnshareServlet (ResourceService resourceServic) throws IOException {
      super(resourceServic);
  }

  public void init () throws UnavailableException {
      super.init();
      warPath = getServletContext().getRealPath("");
      try {
	  Properties prop = new Properties();
	  String configPath = warPath + "/WEB-INF/config/config.properties";
	  prop.load(new InputStreamReader(new FileInputStream(configPath),"UTF-8"));
	  String storageType = prop.getProperty("zipnshare.storageType");
	  if (storageType.equals("localFile")) {
	      String uploadPath = prop.getProperty("zipnshare.uploadPath");
	      dataStorage = new FileStorage(uploadPath);
	  } else if (storageType.equals("azureBlob")) {
	      String azureBlobCS = prop.getProperty("zipnshare.azureBlobCS");
	      String azureBlobContainer = prop.getProperty("zipnshare.azureBlobContainer");
	      dataStorage = new AzureBlobStorage(azureBlobCS, azureBlobContainer);
	  } else {
	      throw new IOException ("invalid storageType");
	  }
      } catch (IOException ex) {
	  // [MEMO] just treated as a 404 error
	  throw new UnavailableException ("failed to load config properties");
      }
  }
    
  private void renderHtml(String localPath, Template.PlaceHolderHandler values, HttpServletResponse res) throws IOException {
      InputStream tempIn = new FileInputStream(warPath + localPath);
      Template temp = Template.parse(tempIn);
      res.setContentType("text/html");
      temp.apply(values,res.getOutputStream());
  }
    
  private void renderDownload(Template.PlaceHolderHandler values, HttpServletResponse res) throws IOException {
      renderHtml("/zipnshare/download.html",values,res);
  }
    
  private String get1stPassword(Collection<Part> parts) throws IOException {
      for (Part part : parts) {
	  if(part.getName().equals("password") && part.getContentType() == null) {
	      ByteArrayOutputStream bout = new ByteArrayOutputStream();
	      Util.copy(part.getInputStream(),bout,32);
	      return new String(bout.toByteArray(),"UTF-8");
	  }
      }
      return null;
  }
    
  private void saveZipFile(ZipWriter zipw, Collection<Part> parts) throws IOException {
      for (Part part : parts) {
	  if(part.getName().equals("file") && part.getContentType() != null) {
	      zipw.append(part.getSubmittedFileName(),part.getInputStream());
	  }
      }
      zipw.close();
  }
    
  public void service(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {

      Router router = new Router(req);
      if (router.matches("POST","/upload")) {
	  String filekey = Util.genAlphaNumericKey(16);
	  String outfilename = filekey + ".zip";
	  
	  Collection<Part> parts = req.getParts();
	  
	  String password = get1stPassword(parts);
	  ZipWriter zipw;
	  OutputStream dataOut = dataStorage.getUploadStream(outfilename);
	  if (password == null || password.equals("") ) {
	      zipw = new ZipWriter (dataOut);
	  } else {
	      zipw = new ZipWriter (dataOut,password);
	  }
	  saveZipFile (zipw, parts);
	  
	  res.setContentType("text/plain");
	  res.getWriter().print(filekey);
	  
      } else if (router.matches("GET","/download_(\\w+).html")) {
	  Matcher m = router.getMatcher();
	  String filekey = m.group(1);
	  // [TODO] check file existance
	  HtmlPlaceHolderHandler values = new HtmlPlaceHolderHandler();
	  values.put("filekey",filekey);
	  renderDownload(values, res);
      } else if (router.matches("GET","/download/(\\w+)")) {
	  Matcher m = router.getMatcher();
	  String filekey = m.group(1);
	  String infilename = filekey + ".zip";

	  res.setContentType("application/zip");
//	  res.setContentType("application/octet-stream");
	  res.setHeader("Content-Disposition", "attachment; filename=" + infilename);
	  dataStorage.download(infilename, res.getOutputStream());
      } else {
	  super.service(req, res); // serv file content
      }
  }


  public static class FileStorage implements DataStorage {
      private String uploadPath;
      public FileStorage (String uploardPath) {
	  this.uploadPath = uploardPath;
      }
      public OutputStream getUploadStream (String filename) throws IOException {
	  String outfilepath = uploadPath + "/" + filename;
	  return new FileOutputStream(outfilepath);
      }
      private InputStream getDownloadStream (String filename) throws IOException {
	  String infilepath = uploadPath + "/" + filename;
	  return new FileInputStream(infilepath);
      }
      public void download (String filename, OutputStream out) throws IOException {
	  InputStream in = getDownloadStream (filename);
	  Util.copy(in,out,1024);
      }
      public boolean exists (String filename) throws IOException {
	  // [TODO]
	  return false;
      }
      public void deleteExpired () throws IOException {
	  // [TODO]
      }
  }

  public static class AzureBlobStorage implements DataStorage {
      private CloudBlobClient blobClient;
      private String containerName;
      public AzureBlobStorage (String azureBlobCS, String containerName) throws IOException {
	  try {
	      CloudStorageAccount storageAccount = CloudStorageAccount.parse(azureBlobCS);
	      blobClient = storageAccount.createCloudBlobClient();
	  } catch (URISyntaxException | InvalidKeyException  ex) {
	      throw new IOException("failed to create blobClient", ex);
	  }
	  this.containerName = containerName;
      }
      public OutputStream getUploadStream (String filename) throws IOException {
	  try {
	      CloudBlobContainer container = blobClient.getContainerReference(containerName);
	      CloudBlockBlob blob = container.getBlockBlobReference(filename);
	      return blob.openOutputStream();
	  } catch (URISyntaxException | StorageException ex) {
	      throw new IOException("failed to open upload OutputStream", ex);
	  }
      }
      public void download (String filename, OutputStream out) throws IOException {
	  try {
	      CloudBlobContainer container = blobClient.getContainerReference(containerName);
	      CloudBlockBlob blob = container.getBlockBlobReference(filename);
	      blob.download(out);
	  } catch (URISyntaxException | StorageException ex) {
	      throw new IOException("failed to download blob", ex);
	  }
      }
      public boolean exists (String filename) throws IOException {
	  // [TODO]
	  return false;
      }
      public void deleteExpired () throws IOException {
	  // [TODO]
      }
  }

/* v12
  //import com.azure.storage.blob.*;
  //import com.azure.storage.blob.models.*;

  public static class AzureBlobStorage implements DataStorage {
      private BlobServiceClient blobServiceClient;
      private String containerName;
      public AzureBlobStorage (String azureBlobCS, String conainerName) {
	  blobServiceClient = new BlobServiceClientBuilder().connectionString(azureBlobCS).buildClient();
	  this.containerName = containerName;
      }
      public OutputStream getUploadStream (String filename) throws IOException {
	  BlobContainerClient containerClient = blobServiceClient.createBlobContainer(containerName);
	  BlobClient blobClient = containerClient.getBlobClient(filename);
// [TODO] get output stream
//	  return blobClient.openOutputStream();
	  return null;
      }
      private InputStream getDownloadStream (String filename) throws IOException {
	  BlobContainerClient containerClient = blobServiceClient.createBlobContainer(containerName);	  
	  BlobClient blobClient = containerClient.getBlobClient(filename);
	  return blobClient.openInputStream();
      }
      public void download (String filename, OutputStream out) throws IOException {
	  BlobContainerClient containerClient = blobServiceClient.createBlobContainer(containerName);	  
	  BlobClient blobClient = containerClient.getBlobClient(filename);
	  blobClient.download(out);
      }
      public boolean exists (String filename) throws IOException {
	  // [TODO]
	  return false;
      }
      public void deleteExpired () throws IOException {
	  // [TODO]
      }
  }
*/
}
