package azure;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;

import type.FileListItem;

public class DatabaseManager {

	private static class File {
	    public File() {
	    }
	    public String getFileName() {
		return fileName;
	    }
	    public void setFileName(String val) {
		fileName = val;
	    }
	    public String getContentType() {
		return contentType;
	    }
	    public void setContentType(String val) {
		contentType = val;
	    }
	    private String fileName;
	    private String contentType;
	}

	private static class Session {
	    public Session() {
	    }
	    public String getId() {
		return id;
	    }
	    public void setId(String val) {
		id = val;
	    }
	    public long getCreatedAt() {
		return createdAt;
	    }
	    public void setCreatedAt(long val) {
		createdAt = val;
	    }
	    public File[] getFiles() {
		return files;
	    }
	    public void setFiles(File[] val) {
		files = val;
	    }
	    public String getOwnerKey() {
		return ownerKey;
	    }
	    public void setOwnerKey(String val) {
		ownerKey = val;
	    }
	    public boolean getLocked() {
		return locked;
	    }
	    public void setLocked(boolean val) {
		locked = val;
	    }
	    public boolean getZiped() {
		return ziped;
	    }
	    public void setZiped(boolean val) {
		ziped = val;
	    }
	    private String id;
	    private long createdAt;
	    private File [] files;
	    private String ownerKey;
	    private boolean locked;
	    private boolean ziped;
	}

	private static final String containerName = "sessions";
	private CosmosClient cosmosClient;
	private String cosmosDatabase;
	private String sessionKey;
	public DatabaseManager (CosmosClient cosmosClient, String cosmosDatabase, String sessionKey ) {
	    this.cosmosClient = cosmosClient;
	    this.cosmosDatabase = cosmosDatabase;
	    this.sessionKey = sessionKey;
	}

	public static void createContainerIfNotExists (CosmosClient cosmosClient, String cosmosDatabase) {
	    CosmosContainerProperties containerProps = new CosmosContainerProperties(containerName, "/id");
//	    ThroughputProperties throughputProps = ThroughputProperties.createManualThroughput(400);
//	    cosmosClient.getDatabase(cosmosDatabase).createContainerIfNotExists(containerProps, throughputProps);
	    cosmosClient.getDatabase(cosmosDatabase).createContainerIfNotExists(containerProps);
	}

	private CosmosContainer getContainer() {
	    return cosmosClient.getDatabase(cosmosDatabase).getContainer(containerName);
	}
	private void set (Session session) {
	    CosmosContainer container = getContainer();
	    container.replaceItem(session,sessionKey,new PartitionKey(sessionKey),new CosmosItemRequestOptions());
	}
	private Session get () {
	    CosmosContainer container = getContainer();
	    Session session = container.readItem(sessionKey,new PartitionKey(sessionKey),Session.class).getItem();
	    return session;
	}
	
	public void create () {
	    Session session = new Session();
	    session.setId(sessionKey);
	    session.setCreatedAt(System.currentTimeMillis());
	    session.setFiles(new File [0]);
	    session.setOwnerKey(null);
	    session.setLocked(false);

	    CosmosContainer container = getContainer();
	    container.createItem(session);
	}
	
	public boolean exists () {
	    List<CosmosItemIdentity> ids = new ArrayList<CosmosItemIdentity>();
	    ids.add(new CosmosItemIdentity(new PartitionKey(sessionKey),sessionKey));
	    
	    CosmosContainer container = getContainer();
	    FeedResponse<Session> res = container.readMany(ids,Session.class);
	    return !res.getResults().isEmpty();
	}
	
	public void setOwnerKey (String ownerKey) {
	    Session session = get ();
	    session.setOwnerKey(ownerKey);
	    set(session);
	}
	
	public boolean matchOwnerKey (String ownerKey) {
	    Session session = get ();
	    return ownerKey.equals(session.getOwnerKey());
	}
	
	public int appendFile (String fileName, String contentType) {
	    Session session = get ();
	    List<File> files = new ArrayList<File>(Arrays.asList(session.getFiles()));
	    File file = new File();
	    file.setFileName(fileName);
	    file.setContentType(contentType);
	    files.add(file);
	    session.setFiles(files.toArray(new File[0]));
	    set(session);
	    return files.size()-1; // [MEMO] return last index for new fileId
	}
	
	public int getFileCount () {
	    Session session = get ();
	    return session.getFiles().length;
	}
	
	public List<FileListItem> getFileList () {
	    Session session = get ();
	    File [] files = session.getFiles();
	    List<FileListItem> result = new ArrayList<FileListItem>();
	    for( File file : files) {
		FileListItem item = new FileListItem(file.getFileName(),file.getContentType());
		result.add(item);
	    }
	    return result;
	}
	
	public boolean isFileNameUsed (String fileName) {
	    boolean result = false;
	    List<FileListItem> files = getFileList();
	    for (FileListItem item : files) {
		if (item.fileName.equals(fileName)) {
		    result = true;
		    break;
		}
	    }
	    return result;
	}
	
	public boolean hasFile (int fileId) {
	    return getFileCount() > fileId;
	}
	
	public FileListItem getFileInfo (int fileId) {
	    Session session = get ();
	    File [] files = session.getFiles();
	    File file = files [fileId];
	    return new FileListItem(file.getFileName(),file.getContentType());
	}
	
	public void lock () {
	    Session session = get ();
	    session.setLocked(true);
	    set(session);
	}
	
	public boolean locked () {
	    Session session = get ();
	    return session.getLocked();
	}
	
	public void delete () {
	    CosmosContainer container = getContainer();
	    container.deleteItem(sessionKey,new PartitionKey(sessionKey),new CosmosItemRequestOptions());
	}
	
	public boolean ziped () {
	    Session session = get ();
	    return session.getZiped();
	}

	public void zip () {
	    Session session = get ();
	    session.setZiped(true);
	    set(session);
	}

	public long getCreatedAt () {
	    Session session = get ();
	    return session.getCreatedAt();
	}

}
    
