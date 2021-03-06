package azure;

import java.util.List;
import java.util.ArrayList;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import com.azure.cosmos.*;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.queue.*;

import org.mitsuji.vswf.Util;
import type.FileListItem;
import type.DataStorage;

public class AzureBlobStorageV8 implements DataStorage {

    private int keyLength;
    private CosmosClient cosmosClient;
    private String cosmosDatabase;
    private CloudBlobClient cloudBlobClient;
    private String cloudBlobContainer;
    private CloudQueueClient cloudQueueClient;
    private String queueName;
    private int maxFileCount;
    private long maxFileSize;
    private boolean useZipConverter;

    public AzureBlobStorageV8(int keyLength, String cosmosAccountEndpoint, String cosmosAccountKey,
            String cosmosDatabase, String storageAccountCS, String cloudBlobContainer, String queueName,
            int maxFileCount, long maxFileSize, boolean useZipConverter) throws DataStorageException {
        this.keyLength = keyLength;
        cosmosClient = new CosmosClientBuilder().endpoint(cosmosAccountEndpoint).key(cosmosAccountKey).buildClient();
        this.cosmosDatabase = cosmosDatabase;
        try {
            cloudBlobClient = CloudStorageAccount.parse(storageAccountCS).createCloudBlobClient();
        } catch (URISyntaxException | InvalidKeyException ex) {
            throw new DataStorageException("failed to create cloudBlobClient", ex);
        }
        this.cloudBlobContainer = cloudBlobContainer;
        if (useZipConverter) {
            try {
                cloudQueueClient = CloudStorageAccount.parse(storageAccountCS).createCloudQueueClient();
            } catch (URISyntaxException | InvalidKeyException ex) {
                throw new DataStorageException("failed to create cloudQueueClient", ex);
            }
            this.queueName = queueName;
        }
        this.maxFileCount = maxFileCount;
        this.maxFileSize = maxFileSize;
        this.useZipConverter = useZipConverter;
    }

    public void init() {
        DatabaseManager.createContainerIfNotExists(cosmosClient, cosmosDatabase);
    }

    public void destroy() {
        cosmosClient.close();
    }

    public String createSession() throws DataStorageException {
        String sessionKey;
        try {
            sessionKey = Util.genAlphaNumericKey(keyLength);
        } catch (NoSuchAlgorithmException ex) {
            throw new DataStorageException("failed to genAlphaNumericKey", ex);
        }
        DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
        dm.create();
        return sessionKey;
    }

    public void setOwnerKey(String sessionKey, String ownerKey) throws DataStorageException {
        DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
        if (!dm.exists()) {
            throw new NoSuchSessionException("failed to setOwnerKey: invalid session key");
        }
        dm.setOwnerKey(ownerKey);
    }

    public String createFileData(String sessionKey, String fileName, String contentType) throws DataStorageException {
        try {
            DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
            BlobManagerV8 bm = new BlobManagerV8(cloudBlobClient, cloudBlobContainer, sessionKey);
            if (!dm.exists()) {
                throw new NoSuchSessionException("failed to crateFileData: invalid session key");
            }
            // check file count limit
            int fileCount = dm.getFileCount();
            if (fileCount >= maxFileCount) {
                throw new TooManyFilesException("fiailed to crateFileData: too many files");
            }
            // check fileName duplication
            if (dm.isFileNameUsed(fileName)) {
                throw new DuplicatedFileNameException("fiailed to crateFileData: duplicated file name");
            }
            int fileId = dm.appendFile(fileName, contentType);
            bm.appendFileData(fileId);
            return Integer.toString(fileId);
        } catch (URISyntaxException | StorageException ex) {
            throw new DataStorageException("failed to createFileData", ex);
        }
    }

    public void upload(String sessionKey, String fileId, InputStream in, long len) throws DataStorageException {
        try {
            DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
            BlobManagerV8 bm = new BlobManagerV8(cloudBlobClient, cloudBlobContainer, sessionKey);
            if (!dm.exists()) {
                throw new NoSuchSessionException("failed to upload: invalid session key");
            }
            if (!dm.hasFile(Integer.valueOf(fileId))) {
                throw new NoSuchFileDataException("failed to upload: invalid fileId");
            }
            long fileSizeBefore = bm.getFileSize(Integer.valueOf(fileId));
            long fileSizeAfter = fileSizeBefore + len;
            if (fileSizeAfter > maxFileSize) {
                throw new TooLargeFileException("failed to upload: too large file");
            }
            bm.upload(Integer.valueOf(fileId), in, len);
        } catch (URISyntaxException | StorageException | IOException ex) {
            throw new DataStorageException("failed to upload", ex);
        }
    }

    public void closeFileData(String sessionKey, String fileId) throws DataStorageException {
        // do nothing
    }

    public void lockSession(String sessionKey) throws DataStorageException {
        DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
        if (!dm.exists()) {
            throw new NoSuchSessionException("failed to lockSession: invalid session key");
        }
        if (dm.locked()) {
            throw new DataStorageException("failed to lockSession: session already locked");
        }
        dm.lock();
        if (useZipConverter) {
            try {
                CloudQueue queue = cloudQueueClient.getQueueReference(queueName);
                queue.addMessage(new CloudQueueMessage(sessionKey));
            } catch (URISyntaxException | StorageException ex) {
                throw new DataStorageException("failed to lockSession: queue.addMessage", ex);
            }
        }
    }

    public boolean hasLocked(String sessionKey) throws DataStorageException {
        DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
        if (!dm.exists()) {
            throw new NoSuchSessionException("failed to hasLocked: invalid session key");
        }
        return dm.locked();
    }

    public long getFileSize(String sessionKey, String fileId) throws DataStorageException {
        try {
            DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
            BlobManagerV8 bm = new BlobManagerV8(cloudBlobClient, cloudBlobContainer, sessionKey);
            if (!dm.exists()) {
                throw new NoSuchSessionException("failed to getFileSize: invalid session key");
            }
            if (!dm.hasFile(Integer.valueOf(fileId))) {
                throw new NoSuchFileDataException("failed to getFileSize: invalid fileId");
            }
            return bm.getFileSize(Integer.valueOf(fileId));
        } catch (URISyntaxException | StorageException ex) {
            throw new DataStorageException("failed to getFileSize", ex);
        }
    }

    public List<FileListItem> getFileList(String sessionKey) throws DataStorageException {
        DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
        if (!dm.exists()) {
            throw new NoSuchSessionException("failed to getFileList: invalid session key");
        }
        return dm.getFileList();
    }

    public FileListItem getFileInfo(String sessionKey, String fileId) throws DataStorageException {
        DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
        if (!dm.exists()) {
            throw new NoSuchSessionException("failed to getFileInfo: invalid session key");
        }
        if (!dm.hasFile(Integer.valueOf(fileId))) {
            throw new NoSuchFileDataException("failed to getFileInfo: invalid fileId");
        }
        return dm.getFileInfo(Integer.valueOf(fileId));
    }

    public void download(String sessionKey, String fileId, OutputStream out) throws DataStorageException {
        try {
            DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
            BlobManagerV8 bm = new BlobManagerV8(cloudBlobClient, cloudBlobContainer, sessionKey);
            if (!dm.exists()) {
                throw new NoSuchSessionException("failed to download: invalid session key");
            }
            if (!dm.hasFile(Integer.valueOf(fileId))) {
                throw new NoSuchFileDataException("failed to download: invalid fileId");
            }
            bm.download(Integer.valueOf(fileId), out);
        } catch (URISyntaxException | StorageException | IOException ex) {
            throw new DataStorageException("failed to download", ex);
        }
    }

    public boolean matchOwnerKey(String sessionKey, String ownerKey) throws DataStorageException {
        DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
        if (!dm.exists()) {
            throw new NoSuchSessionException("failed to matchOwnerKey: invalid session key");
        }
        return dm.matchOwnerKey(ownerKey);
    }

    public void deleteSession(String sessionKey) throws DataStorageException {
        try {
            DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
            BlobManagerV8 bm = new BlobManagerV8(cloudBlobClient, cloudBlobContainer, sessionKey);
            if (!dm.exists()) {
                throw new NoSuchSessionException("failed to deleteSession: invalid session key");
            }
            bm.deleteAll();
            dm.delete();
        } catch (URISyntaxException | StorageException ex) {
            throw new DataStorageException("failed to deleteSession", ex);
        }

    }

    public boolean hasZiped(String sessionKey) throws DataStorageException {
        DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
        if (!dm.exists()) {
            throw new NoSuchSessionException("failed to hasZiped: invalid session key");
        }
        return dm.ziped();
    }

    public long getZipFileSize(String sessionKey) throws DataStorageException {
        try {
            DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
            BlobManagerV8 bm = new BlobManagerV8(cloudBlobClient, cloudBlobContainer, sessionKey);
            if (!dm.exists()) {
                throw new NoSuchSessionException("failed to getZipFileSize: invalid session key");
            }
            if (!dm.ziped()) {
                throw new NoSuchFileDataException("failed to getZipFileSize: invalid fileId");
            }
            return bm.getZipFileSize();
        } catch (URISyntaxException | StorageException ex) {
            throw new DataStorageException("failed to getZipFileSize", ex);
        }
    }

    public void zipDownload(String sessionKey, OutputStream out) throws DataStorageException {
        try {
            DatabaseManager dm = new DatabaseManager(cosmosClient, cosmosDatabase, sessionKey);
            BlobManagerV8 bm = new BlobManagerV8(cloudBlobClient, cloudBlobContainer, sessionKey);
            if (!dm.exists()) {
                throw new NoSuchSessionException("failed to zipDownload: invalid session key");
            }
            if (!dm.ziped()) {
                throw new NoSuchFileDataException("failed to zipDownload: invalid fileId");
            }
            bm.zipDownload(out);
        } catch (URISyntaxException | StorageException | IOException ex) {
            throw new DataStorageException("failed to zipDownload", ex);
        }
    }

}
