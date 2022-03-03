package type;

import java.util.List;

import java.io.InputStream;
import java.io.OutputStream;

public interface DataStorage {
    public static class DataStorageException extends Exception {
        public DataStorageException(String message) {
            super(message);
        }

        public DataStorageException(String message, Throwable cause) {
            super(message, cause);
        }
    };

    public static class NoSuchSessionException extends DataStorageException {
        public NoSuchSessionException(String message) {
            super(message);
        }
    };

    public static class NoSuchFileDataException extends DataStorageException {
        public NoSuchFileDataException(String message) {
            super(message);
        }
    };

    public static class TooManyFilesException extends DataStorageException {
        public TooManyFilesException(String message) {
            super(message);
        }
    };

    public static class DuplicatedFileNameException extends DataStorageException {
        public DuplicatedFileNameException(String message) {
            super(message);
        }
    };

    public static class TooLargeFileException extends DataStorageException {
        public TooLargeFileException(String message) {
            super(message);
        }
    };

    public String createSession() throws DataStorageException;

    public void setOwnerKey(String sessionKey, String ownerKey) throws DataStorageException;

    public String createFileData(String sessionKey, String fileName, String contentType) throws DataStorageException;

    public void upload(String sessionKey, String fileId, InputStream in, long len) throws DataStorageException;

    public void closeFileData(String sessionKey, String fileId) throws DataStorageException;

    public void lockSession(String sessionKey) throws DataStorageException;

    public boolean hasLocked(String sessionKey) throws DataStorageException;

    public long getFileSize(String sessionKey, String fileId) throws DataStorageException;

    public List<FileListItem> getFileList(String sessionKey) throws DataStorageException;

    public FileListItem getFileInfo(String sessionKey, String fileId) throws DataStorageException;

    public void download(String sessionKey, String fileId, OutputStream out) throws DataStorageException;

    public boolean matchOwnerKey(String sessionKey, String ownerKey) throws DataStorageException;

    public void deleteSession(String sessionKey) throws DataStorageException;

    public boolean hasZiped(String sessionKey) throws DataStorageException;

    public long getZipFileSize(String sessionKey) throws DataStorageException;

    public void zipDownload(String sessionKey, OutputStream out) throws DataStorageException;

    public void destroy();
}
