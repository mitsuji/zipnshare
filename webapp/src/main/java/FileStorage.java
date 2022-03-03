import java.util.List;
import java.util.ArrayList;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import java.security.NoSuchAlgorithmException;

import org.mitsuji.vswf.Util;
import org.mitsuji.vswf.ZipWriter;
import type.FileListItem;
import type.DataStorage;
import type.BackgroundJob;
import type.ZipQueueProcessor;

public class FileStorage implements DataStorage {

    private static class FileManager {
        private String sessionDirPath;

        public FileManager(String uploadPath, String sessionKey) {
            sessionDirPath = uploadPath + "/" + sessionKey;
        }

        private String getFileListFilePath() {
            return sessionDirPath + "/_files";
        }

        private String getCreatedatFilePath() {
            return sessionDirPath + "/_createdat";
        }

        private String getOwnerKeyFilePath() {
            return sessionDirPath + "/ownerKey";
        }

        public String getFileDataFilePath(int fileId) {
            return sessionDirPath + "/" + Integer.toString(fileId);
        }

        private String getLockedFilePath() {
            return sessionDirPath + "/_locked";
        }

        public String getZipFilePath() {
            return sessionDirPath + "/zip";
        }

        private String getZipedFilePath() {
            return sessionDirPath + "/_ziped";
        }

        public void createCreatedatFile() throws IOException {
            // create session directory
            Files.createDirectory(Paths.get(sessionDirPath));
            // create _createdat file
            OutputStream out = new FileOutputStream(getCreatedatFilePath());
            out.write(Long.toString(System.currentTimeMillis()).getBytes("UTF-8")); // unix epoch
            out.close();
        }

        public void createFileListFile() throws IOException {
            // create _files file
            Files.createFile(Paths.get(getFileListFilePath()));
        }

        public void createOwnerKeyFile(String ownerKey) throws IOException {
            // create ownerKey file
            OutputStream out = new FileOutputStream(getOwnerKeyFilePath());
            out.write(ownerKey.getBytes("UTF-8"));
            out.close();
        }

        public int getFileCount() throws IOException {
            int lc = Util.getLineCount(new FileInputStream(getFileListFilePath()));
            return lc / 2; // 2 lines for each file
        }

        public void appendFileData(int fileId, String fileName, String contentType) throws IOException {
            // create file data file
            Files.createFile(Paths.get(getFileDataFilePath(fileId)));
            OutputStream out = new FileOutputStream(getFileListFilePath(), true);
            out.write(fileName.trim().getBytes("UTF-8"));
            out.write('\n');
            out.write(contentType.trim().getBytes("UTF-8"));
            out.write('\n');
            out.close();
        }

        public void upload(int fileId, InputStream in, long len) throws IOException {
            OutputStream out = new FileOutputStream(getFileDataFilePath(fileId), true);
            Util.copy(in, out, 1024 * 1024);
            out.close();
        }

        public void createLockedFile() throws IOException {
            // create locked file
            Files.createFile(Paths.get(getLockedFilePath()));
        }

        public void createZipedFile() throws IOException {
            // create ziped file
            Files.createFile(Paths.get(getZipedFilePath()));
        }

        public List<FileListItem> getFileList() throws IOException {
            List<FileListItem> result = new ArrayList<FileListItem>();
            InputStream in = new FileInputStream(getFileListFilePath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            int i = 0;
            String line1;
            String line2;
            while ((line1 = reader.readLine()) != null) {
                line2 = reader.readLine(); // [MEMO] 2 lines for each file
                FileListItem item = new FileListItem(line1, line2);
                result.add(item);
                i++;
            }
            return result;
        }

        public FileListItem getFileInfo(int fileId) throws IOException {
            InputStream in = new FileInputStream(getFileListFilePath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            for (int i = 0; i < fileId; i++) {
                reader.readLine(); // [TODO] check null ?
                reader.readLine(); // [TODO] check null ?
            }
            String fileName = reader.readLine();
            String contentType = reader.readLine();
            return new FileListItem(fileName, contentType);
        }

        public InputStream getFileDataInputStream(int fileId) throws IOException {
            return new FileInputStream(getFileDataFilePath(fileId));
        }

        public InputStream getZipFileInputStream() throws IOException {
            return new FileInputStream(getZipFilePath());
        }

        public boolean hasCreatedatFile() {
            return Files.exists(Paths.get(getCreatedatFilePath()));
        }

        public boolean hasLockedFile() {
            return Files.exists(Paths.get(getLockedFilePath()));
        }

        public boolean hasZipedFile() {
            return Files.exists(Paths.get(getZipedFilePath()));
        }

        public boolean hasFileDataFile(int fileId) {
            return Files.exists(Paths.get(getFileDataFilePath(fileId)));
        }

        public long getFileSize(int fileId) throws IOException {
            return Files.size(Paths.get(getFileDataFilePath(fileId)));
        }

        public long getZipFileSize() throws IOException {
            return Files.size(Paths.get(getZipFilePath()));
        }

        public boolean isFileNameUsed(String fileName) throws IOException {
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

        public boolean matchOwnerKey(String ownerKey) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InputStream in = new FileInputStream(getOwnerKeyFilePath());
            Util.copy(in, out, 1024);
            out.close();
            String fileOwnerKey = new String(out.toByteArray(), "UTF-8");
            return ownerKey.equals(fileOwnerKey);
        }

        public void deleteSession() throws IOException {
            // [MEMO] list and delete files
            File dir = new File(sessionDirPath);
            File[] files = dir.listFiles();
            for (File file : files) {
                file.delete();
            }
            // [MEMO] delete dir
            dir.delete();
        }

        public long getCreatedat() throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InputStream in = new FileInputStream(getCreatedatFilePath());
            Util.copy(in, out, 1024);
            out.close();
            String createdAt = new String(out.toByteArray(), "UTF-8");
            return Long.valueOf(createdAt);
        }

    }

    private static class FileBackgroundJob implements BackgroundJob {

        private String uploadPath;

        // for clean ()
        private long cleanExpiredSeconds;
        private long cleanGarbageSeconds;

        public FileBackgroundJob(String uploadPath) {
            this(uploadPath, 0, 0);
        }

        public FileBackgroundJob(String uploadPath, long cleanExpiredSeconds, long cleanGarbageSeconds) {
            this.uploadPath = uploadPath;
            this.cleanExpiredSeconds = cleanExpiredSeconds;
            this.cleanGarbageSeconds = cleanGarbageSeconds;
        }

        public void clean() throws BackgroundJobException {
            try {
                File[] directories = new File(uploadPath).listFiles(File::isDirectory);
                for (File directory : directories) {
                    String sessionKey = directory.getName();
                    FileManager fm = new FileManager(uploadPath, sessionKey);
                    long now = System.currentTimeMillis();
                    long createdAt = fm.getCreatedat();
                    boolean locked = fm.hasLockedFile();
                    boolean expired1 = createdAt + (cleanExpiredSeconds * 1000) < now; // expired
                    boolean expired2 = (!locked) && (createdAt + (cleanGarbageSeconds * 1000) < now); // garbage
                    if (expired1 || expired2) {
                        fm.deleteSession();
                    }
                }
            } catch (IOException ex) {
                throw new BackgroundJobException("failed to clean", ex);
            }
        }

        public void zipConvert(String sessionKey) throws BackgroundJobException {
            FileManager fm = new FileManager(uploadPath, sessionKey);
            if (!fm.hasCreatedatFile()) {
                throw new BackgroundJob.NoSuchSessionException("failed to zipConvert: session missing");
            }
            if (fm.hasZipedFile()) {
                throw new BackgroundJob.NoSuchSessionException("failed to zipConvert: already ziped");
            }
            try {
                // [TODO] zip password
                ZipWriter zw = new ZipWriter(new FileOutputStream(fm.getZipFilePath()));
                List<FileListItem> files = fm.getFileList();
                for (int i = 0; i < files.size(); i++) {
                    FileListItem file = files.get(i);
                    zw.append(file.fileName, new FileInputStream(fm.getFileDataFilePath(i)));
                }
                zw.close();
                fm.createZipedFile();
            } catch (IOException ex) {
                throw new BackgroundJobException("failed to zipConvert", ex);
            }
        }

    }

    private static class FileZipQueueProcessor implements ZipQueueProcessor {
        private BackgroundJob backgroundJob;
        private String uploadPath;
        private Queue<String> queue;

        public FileZipQueueProcessor(String uploadPath, Queue<String> queue) {
            backgroundJob = new FileBackgroundJob(uploadPath);
            this.uploadPath = uploadPath;
            this.queue = queue;
        }

        public void process() throws Exception {
            String sessionKey = queue.poll();
            if (sessionKey != null) {

                boolean succeed = false;
                try {
                    backgroundJob.zipConvert(sessionKey);
                    succeed = true;
                    // } catch (BackgroundJob.NoSuchSessionException ex) {
                    // // [TODO] log
                    // ex.printStackTrace();
                    // succeed = true; // [MEMO] to delete from queue
                    // } catch (BackgroundJob.BackgroundJobException ex) {
                    // // [TODO] log
                    // ex.printStackTrace();
                } catch (Exception ex) {
                    // [MEMO] allways delete from queue when fail
                    // [TODO] log
                    ex.printStackTrace();
                    succeed = true; // [MEMO] to delete from queue
                }

                if (!succeed) {
                    queue.add(sessionKey);
                }
            }
        }

    }

    private int keyLength;
    private String uploadPath;
    private Queue<String> queue;
    private Thread cleanerThread;
    private Thread zipConverterThread;
    private int maxFileCount;
    private long maxFileSize;
    private boolean useZipConverter;

    public FileStorage(int keyLength, String uploadPath, int maxFileCount, long maxFileSize, boolean useZipConverter) {
        this.keyLength = keyLength;
        this.uploadPath = uploadPath;
        this.maxFileCount = maxFileCount;
        this.maxFileSize = maxFileSize;
        this.useZipConverter = useZipConverter;
    }

    public void init(long cleanIntervalSeconds, long cleanExpiredSeconds, long cleanGarbageSeconds,
            long zipConvertIntervalSeconds) {
        cleanerThread = new Thread(new Cleaner(cleanIntervalSeconds,
                new FileBackgroundJob(uploadPath, cleanExpiredSeconds, cleanGarbageSeconds)));
        cleanerThread.start();
        if (useZipConverter) {
            queue = new ConcurrentLinkedQueue<String>();
            zipConverterThread = new Thread(
                    new ZipConverter(zipConvertIntervalSeconds, new FileZipQueueProcessor(uploadPath, queue)));
            zipConverterThread.start();
        }
    }

    public void destroy() {
        if (useZipConverter) {
            zipConverterThread.interrupt();
        }
        cleanerThread.interrupt();
    }

    public String createSession() throws DataStorageException {
        String sessionKey;
        try {
            sessionKey = Util.genAlphaNumericKey(keyLength);
        } catch (NoSuchAlgorithmException ex) {
            throw new DataStorageException("failed to genAlphaNumericKey", ex);
        }
        try {
            FileManager fm = new FileManager(uploadPath, sessionKey);
            fm.createCreatedatFile();
            fm.createFileListFile();
        } catch (IOException ex) {
            throw new DataStorageException("failed to createSession", ex);
        }
        return sessionKey;
    }

    public void setOwnerKey(String sessionKey, String ownerKey) throws DataStorageException {
        try {
            FileManager fm = new FileManager(uploadPath, sessionKey);
            if (!fm.hasCreatedatFile()) {
                throw new NoSuchSessionException("failed to setOwnerKey: invalid session key");
            }
            fm.createOwnerKeyFile(ownerKey);
        } catch (IOException ex) {
            throw new DataStorageException("failed to setOwnerKey", ex);
        }
    }

    public String createFileData(String sessionKey, String fileName, String contentType) throws DataStorageException {
        try {
            FileManager fm = new FileManager(uploadPath, sessionKey);
            if (!fm.hasCreatedatFile()) {
                throw new NoSuchSessionException("failed to crateFileData: invalid session key");
            }
            // check file count limit
            int fileCount = fm.getFileCount();
            if (fileCount >= maxFileCount) {
                throw new TooManyFilesException("fiailed to crateFileData: too many files");
            }
            // check fileName duplication
            if (fm.isFileNameUsed(fileName)) {
                throw new DuplicatedFileNameException("fiailed to crateFileData: duplicated file name");
            }
            fm.appendFileData(fileCount, fileName, contentType);
            return Integer.toString(fileCount);
        } catch (IOException ex) {
            throw new DataStorageException("failed to createFileData", ex);
        }
    }

    public void upload(String sessionKey, String fileId, InputStream in, long len) throws DataStorageException {
        try {
            FileManager fm = new FileManager(uploadPath, sessionKey);
            if (!fm.hasCreatedatFile()) {
                throw new NoSuchSessionException("failed to upload: invalid session key");
            }
            if (!fm.hasFileDataFile(Integer.valueOf(fileId))) {
                throw new NoSuchFileDataException("failed to upload: invalid fileId");
            }
            long fileSizeBefore = fm.getFileSize(Integer.valueOf(fileId));
            long fileSizeAfter = fileSizeBefore + len;
            if (fileSizeAfter > maxFileSize) {
                throw new TooLargeFileException("failed to upload: too large file");
            }
            fm.upload(Integer.valueOf(fileId), in, len);
        } catch (IOException ex) {
            throw new DataStorageException("failed to upload", ex);
        }
    }

    public void closeFileData(String sessionKey, String fileId) throws DataStorageException {
        // do nothing
    }

    public void lockSession(String sessionKey) throws DataStorageException {
        try {
            FileManager fm = new FileManager(uploadPath, sessionKey);
            if (!fm.hasCreatedatFile()) {
                throw new NoSuchSessionException("failed to lockSession: invalid session key");
            }
            if (fm.hasLockedFile()) {
                throw new DataStorageException("failed to lockSession: session already locked");
            }
            fm.createLockedFile();
        } catch (IOException ex) {
            throw new DataStorageException("failed to lockSession", ex);
        }
        if (useZipConverter) {
            queue.add(sessionKey);
        }
    }

    public boolean hasLocked(String sessionKey) throws DataStorageException {
        FileManager fm = new FileManager(uploadPath, sessionKey);
        if (!fm.hasCreatedatFile()) {
            throw new NoSuchSessionException("failed to hasLocked: invalid session key");
        }
        return fm.hasLockedFile();
    }

    public long getFileSize(String sessionKey, String fileId) throws DataStorageException {
        try {
            FileManager fm = new FileManager(uploadPath, sessionKey);
            if (!fm.hasCreatedatFile()) {
                throw new NoSuchSessionException("failed to getFileSize: invalid session key");
            }
            if (!fm.hasFileDataFile(Integer.valueOf(fileId))) {
                throw new NoSuchFileDataException("failed to getFileSize: invalid fileId");
            }
            return fm.getFileSize(Integer.valueOf(fileId));
        } catch (IOException ex) {
            throw new DataStorageException("failed to getFileSize", ex);
        }
    }

    public List<FileListItem> getFileList(String sessionKey) throws DataStorageException {
        try {
            FileManager fm = new FileManager(uploadPath, sessionKey);
            if (!fm.hasCreatedatFile()) {
                throw new NoSuchSessionException("failed to getFileList: invalid session key");
            }
            return fm.getFileList();
        } catch (IOException ex) {
            throw new DataStorageException("failed to getFileList", ex);
        }
    }

    public FileListItem getFileInfo(String sessionKey, String fileId) throws DataStorageException {
        try {
            FileManager fm = new FileManager(uploadPath, sessionKey);
            if (!fm.hasCreatedatFile()) {
                throw new NoSuchSessionException("failed to getFileInfo: invalid session key");
            }
            if (!fm.hasFileDataFile(Integer.valueOf(fileId))) {
                throw new NoSuchFileDataException("failed to getFileInfo: invalid fileId");
            }
            return fm.getFileInfo(Integer.valueOf(fileId));
        } catch (IOException ex) {
            throw new DataStorageException("failed to getFileInfo", ex);
        }
    }

    public void download(String sessionKey, String fileId, OutputStream out) throws DataStorageException {
        try {
            FileManager fm = new FileManager(uploadPath, sessionKey);
            if (!fm.hasCreatedatFile()) {
                throw new NoSuchSessionException("failed to download: invalid session key");
            }
            if (!fm.hasFileDataFile(Integer.valueOf(fileId))) {
                throw new NoSuchFileDataException("failed to download: invalid fileId");
            }
            InputStream in = fm.getFileDataInputStream(Integer.valueOf(fileId));
            Util.copy(in, out, 1024 * 1024);
        } catch (IOException ex) {
            throw new DataStorageException("failed to download", ex);
        }
    }

    public boolean matchOwnerKey(String sessionKey, String ownerKey) throws DataStorageException {
        try {
            FileManager fm = new FileManager(uploadPath, sessionKey);
            if (!fm.hasCreatedatFile()) {
                throw new NoSuchSessionException("failed to matchOwnerKey: invalid session key");
            }
            return fm.matchOwnerKey(ownerKey);
        } catch (IOException ex) {
            throw new DataStorageException("failed to matchOwnerKey", ex);
        }
    }

    public void deleteSession(String sessionKey) throws DataStorageException {
        try {
            FileManager fm = new FileManager(uploadPath, sessionKey);
            if (!fm.hasCreatedatFile()) {
                throw new NoSuchSessionException("failed to deleteSession: invalid session key");
            }
            fm.deleteSession();
        } catch (IOException ex) {
            throw new DataStorageException("failed to deleteSession", ex);
        }

    }

    public boolean hasZiped(String sessionKey) throws DataStorageException {
        FileManager fm = new FileManager(uploadPath, sessionKey);
        if (!fm.hasCreatedatFile()) {
            throw new NoSuchSessionException("failed to hasZiped: invalid session key");
        }
        return fm.hasZipedFile();
    }

    public long getZipFileSize(String sessionKey) throws DataStorageException {
        FileManager fm = new FileManager(uploadPath, sessionKey);
        if (!fm.hasCreatedatFile()) {
            throw new NoSuchSessionException("failed to getZipFileSize: invalid session key");
        }
        if (!fm.hasZipedFile()) {
            throw new DataStorageException("failed to getZipFileSize: no zip file");
        }
        try {
            return fm.getZipFileSize();
        } catch (IOException ex) {
            throw new DataStorageException("failed to getZipFileSize", ex);
        }
    }

    public void zipDownload(String sessionKey, OutputStream out) throws DataStorageException {
        try {
            FileManager fm = new FileManager(uploadPath, sessionKey);
            if (!fm.hasCreatedatFile()) {
                throw new NoSuchSessionException("failed to zipDownload: invalid session key");
            }
            if (!fm.hasZipedFile()) {
                throw new DataStorageException("failed to zipDownload: no zip file");
            }
            InputStream in = fm.getZipFileInputStream();
            Util.copy(in, out, 1024 * 1024);
        } catch (IOException ex) {
            throw new DataStorageException("failed to zipDownload", ex);
        }
    }

}
