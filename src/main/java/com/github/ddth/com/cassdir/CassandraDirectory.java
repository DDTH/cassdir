package com.github.ddth.com.cassdir;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.zip.CRC32;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.ddth.cacheadapter.ICache;
import com.github.ddth.cacheadapter.ICacheFactory;
import com.github.ddth.com.cassdir.internal.CassandraLockFactory;
import com.github.ddth.cql.CqlUtils;
import com.github.ddth.cql.SessionManager;

/**
 * Cassandra implementation of {@link Directory}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class CassandraDirectory extends BaseDirectory {

    public final static int BLOCK_SIZE = 64 * 1024; // 64Kb
    public final static String DEFAULT_TBL_METADATA = "directory_metadata";
    public final static String DEFAULT_TBL_FILEDATA = "file_data";
    public final static ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM;

    private ConsistencyLevel consistencyLevelReadFileData = DEFAULT_CONSISTENCY_LEVEL;
    private ConsistencyLevel consistencyLevelWriteFileData = DEFAULT_CONSISTENCY_LEVEL;
    private ConsistencyLevel consistencyLevelReadFileInfo = DEFAULT_CONSISTENCY_LEVEL;
    private ConsistencyLevel consistencyLevelWriteFileInfo = DEFAULT_CONSISTENCY_LEVEL;
    private ConsistencyLevel consistencyLevelRemoveFileInfo = DEFAULT_CONSISTENCY_LEVEL;
    private ConsistencyLevel consistencyLevelRemoveFileData = DEFAULT_CONSISTENCY_LEVEL;
    private ConsistencyLevel consistencyLevelLock = DEFAULT_CONSISTENCY_LEVEL;

    private String tableFiledata = DEFAULT_TBL_FILEDATA;
    private String tableMetadata = DEFAULT_TBL_METADATA;
    public final static String COL_FILE_NAME = "filename";
    public final static String COL_FILE_SIZE = "filesize";
    public final static String COL_FILE_ID = "fileid";
    public final static String COL_BLOCK_NUM = "blocknum";
    public final static String COL_BLOCK_DATA = "blockdata";

    private String CQL_REMOVE_FILE = "DELETE FROM {0} WHERE " + COL_FILE_NAME + "=?";
    private String CQL_REMOVE_FILEDATA = "DELETE FROM {0} WHERE " + COL_FILE_ID + "=? AND "
            + COL_BLOCK_NUM + "=?";

    private String CQL_LOAD_FILEDATA = "SELECT "
            + StringUtils.join(new String[] { COL_FILE_ID, COL_BLOCK_NUM, COL_BLOCK_DATA }, ",")
            + " FROM {0} WHERE " + COL_FILE_ID + "=? AND " + COL_BLOCK_NUM + "=?";
    private String CQL_WRITE_FILEDATA = "UPDATE {0} SET " + COL_BLOCK_DATA + "=? WHERE "
            + COL_FILE_ID + "=? AND " + COL_BLOCK_NUM + "=?";

    private String CQL_GET_FILEINFO = "SELECT "
            + StringUtils.join(new String[] { COL_FILE_NAME, COL_FILE_SIZE, COL_FILE_ID }, ",")
            + " FROM {0} WHERE " + COL_FILE_NAME + "=?";
    private String CQL_GET_ALL_FILES = "SELECT "
            + StringUtils.join(new String[] { COL_FILE_NAME }, ",") + " FROM {0}";

    private String CQL_ENSURE_FILE = "UPDATE {0} SET " + COL_FILE_ID + "=? WHERE " + COL_FILE_NAME
            + "=?";
    private String CQL_UPDATE_FILEINFO = "UPDATE {0} SET " + COL_FILE_SIZE + "=?," + COL_FILE_ID
            + "=? WHERE " + COL_FILE_NAME + "=?";

    private String CQL_LOCK = "INSERT INTO {0} ("
            + StringUtils.join(new String[] { COL_FILE_NAME, COL_FILE_ID }, ",")
            + ") VALUES (?, ?) IF NOT EXISTS";

    private Logger LOGGER = LoggerFactory.getLogger(CassandraDirectory.class);

    private String cassandraHostsAndPorts;
    private String cassandraKeyspace;
    private String cassandraUser, cassandraPassword;
    private SessionManager sessionManager;
    private boolean myOwnSessionManager = false;

    private ICacheFactory cacheFactory;
    private String cacheName;
    private String cacheKeyAllFiles = "ALL_FILES";

    /*----------------------------------------------------------------------*/
    public CassandraDirectory(String cassandraHostsAndPorts, String cassandraUser,
            String cassandraPassword, String cassandraKeyspace) {
        super(CassandraLockFactory.INSTANCE);
        this.cassandraHostsAndPorts = cassandraHostsAndPorts;
        this.cassandraKeyspace = cassandraKeyspace;
        this.cassandraUser = cassandraUser;
        this.cassandraPassword = cassandraPassword;
        init();
    }

    public String getTableFiledata() {
        return tableFiledata;
    }

    public CassandraDirectory setTableFiledata(String tableFiledata) {
        this.tableFiledata = tableFiledata;
        return this;
    }

    public String getTableMetadata() {
        return tableMetadata;
    }

    public CassandraDirectory setTableMetadata(String tableMetadata) {
        this.tableMetadata = tableMetadata;
        return this;
    }

    public String getCassandraHostsAndPorts() {
        return cassandraHostsAndPorts;
    }

    public CassandraDirectory setCassandraHostsAndPorts(String cassandraHostsAndPorts) {
        this.cassandraHostsAndPorts = cassandraHostsAndPorts;
        return this;
    }

    public String getCassandraKeyspace() {
        return cassandraKeyspace;
    }

    public CassandraDirectory setCassandraKeyspace(String cassandraKeyspace) {
        this.cassandraKeyspace = cassandraKeyspace;
        return this;
    }

    public String getCassandraUser() {
        return cassandraUser;
    }

    public CassandraDirectory setCassandraUser(String cassandraUser) {
        this.cassandraUser = cassandraUser;
        return this;
    }

    public String getCassandraPassword() {
        return cassandraPassword;
    }

    public CassandraDirectory setCassandraPassword(String cassandraPassword) {
        this.cassandraPassword = cassandraPassword;
        return this;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public CassandraDirectory setSessionManager(SessionManager sessionManager) {
        if (this.sessionManager == null || this.sessionManager == sessionManager) {
            myOwnSessionManager = false;
            this.sessionManager = sessionManager;
        } else {
            throw new IllegalStateException("My own session manager has been initialized!");
        }
        return this;
    }

    public ConsistencyLevel getConsistencyLevelReadFileData() {
        return consistencyLevelReadFileData;
    }

    public CassandraDirectory setConsistencyLevelReadFileData(
            ConsistencyLevel consistencyLevelReadFileData) {
        this.consistencyLevelReadFileData = consistencyLevelReadFileData;
        return this;
    }

    public ConsistencyLevel getConsistencyLevelWriteFileData() {
        return consistencyLevelWriteFileData;
    }

    public CassandraDirectory setConsistencyLevelWriteFileData(
            ConsistencyLevel consistencyLevelWriteFileData) {
        this.consistencyLevelWriteFileData = consistencyLevelWriteFileData;
        return this;
    }

    public ConsistencyLevel getConsistencyLevelReadFileInfo() {
        return consistencyLevelReadFileInfo;
    }

    public CassandraDirectory setConsistencyLevelReadFileInfo(
            ConsistencyLevel consistencyLevelReadFileInfo) {
        this.consistencyLevelReadFileInfo = consistencyLevelReadFileInfo;
        return this;
    }

    public ConsistencyLevel getConsistencyLevelWriteFileInfo() {
        return consistencyLevelWriteFileInfo;
    }

    public CassandraDirectory setConsistencyLevelWriteFileInfo(
            ConsistencyLevel consistencyLevelWriteFileInfo) {
        this.consistencyLevelWriteFileInfo = consistencyLevelWriteFileInfo;
        return this;
    }

    public ConsistencyLevel getConsistencyLevelRemoveFileInfo() {
        return consistencyLevelRemoveFileInfo;
    }

    public CassandraDirectory setConsistencyLevelRemoveFileInfo(
            ConsistencyLevel consistencyLevelRemoveFileInfo) {
        this.consistencyLevelRemoveFileInfo = consistencyLevelRemoveFileInfo;
        return this;
    }

    public ConsistencyLevel getConsistencyLevelRemoveFileData() {
        return consistencyLevelRemoveFileData;
    }

    public CassandraDirectory setConsistencyLevelRemoveFileData(
            ConsistencyLevel consistencyLevelRemoveFileData) {
        this.consistencyLevelRemoveFileData = consistencyLevelRemoveFileData;
        return this;
    }

    public ConsistencyLevel getConsistencyLevelLock() {
        return consistencyLevelLock;
    }

    public CassandraDirectory setConsistencyLevelLock(ConsistencyLevel consistencyLevelLock) {
        this.consistencyLevelLock = consistencyLevelLock;
        return this;
    }

    /*----------------------------------------------------------------------*/
    public ICacheFactory getCacheFactory() {
        return cacheFactory;
    }

    public CassandraDirectory setCacheFactory(ICacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
        return this;
    }

    public String getCacheName() {
        return cacheName;
    }

    public CassandraDirectory setCacheName(String cacheName) {
        this.cacheName = cacheName;
        return this;
    }

    private String cacheKeyDataBlock(FileInfo fileInfo, int blockNum) {
        return fileInfo.id() + ":" + blockNum;
    }

    private String cacheKeyFileInfo(FileInfo fileInfo) {
        return fileInfo.name();
    }

    private String cacheKeyFileInfo(String fileName) {
        return fileName;
    }

    /*----------------------------------------------------------------------*/
    public void init() {
        if (sessionManager == null) {
            myOwnSessionManager = true;
            sessionManager = new SessionManager();
            sessionManager.init();
        }
        CQL_REMOVE_FILE = MessageFormat.format(CQL_REMOVE_FILE, tableMetadata);
        CQL_REMOVE_FILEDATA = MessageFormat.format(CQL_REMOVE_FILEDATA, tableFiledata);

        CQL_LOAD_FILEDATA = MessageFormat.format(CQL_LOAD_FILEDATA, tableFiledata);
        CQL_WRITE_FILEDATA = MessageFormat.format(CQL_WRITE_FILEDATA, tableFiledata);

        CQL_GET_FILEINFO = MessageFormat.format(CQL_GET_FILEINFO, tableMetadata);
        CQL_GET_ALL_FILES = MessageFormat.format(CQL_GET_ALL_FILES, tableMetadata);

        CQL_ENSURE_FILE = MessageFormat.format(CQL_ENSURE_FILE, tableMetadata);
        CQL_UPDATE_FILEINFO = MessageFormat.format(CQL_UPDATE_FILEINFO, tableMetadata);

        CQL_LOCK = MessageFormat.format(CQL_LOCK, tableMetadata);
    }

    public void destroy() {
        if (myOwnSessionManager && sessionManager != null) {
            sessionManager.destroy();
            sessionManager = null;
        }
    }

    private Session getSession() {
        return sessionManager.getSession(cassandraHostsAndPorts, cassandraUser, cassandraPassword,
                cassandraKeyspace);
    }

    private ICache getCache() {
        return cacheFactory != null && cacheName != null ? cacheFactory.createCache(cacheName)
                : null;
    }

    /**
     * Loads a file's block data from storage.
     * 
     * @param fileInfo
     * @param blockNum
     * @return {@code null} if file and/or block does not exist, otherwise a
     *         {@code byte[]} with minimum {@link #BLOCK_SIZE} length is
     *         returned
     */
    private byte[] readFileBlock(FileInfo fileInfo, int blockNum) {
        ICache cache = getCache();
        final String CACHE_KEY = cacheKeyDataBlock(fileInfo, blockNum);
        byte[] dataArr = (byte[]) (cache != null ? cache.get(CACHE_KEY) : null);
        if (LOGGER.isTraceEnabled()) {
            if (dataArr != null) {
                LOGGER.trace("readFileBlock(" + fileInfo.name() + " - " + fileInfo.id() + "/"
                        + blockNum + ") --> cache hit!");
            } else {
                LOGGER.trace("readFileBlock(" + fileInfo.name() + " - " + fileInfo.id() + "/"
                        + blockNum + ") --> cache missed!");
            }
        }
        if (dataArr == null) {
            Session session = getSession();
            Row row = CqlUtils.executeOne(session, CQL_LOAD_FILEDATA, consistencyLevelReadFileData,
                    fileInfo.id(), blockNum);
            ByteBuffer data = row != null ? row.getBytes(COL_BLOCK_DATA) : null;
            dataArr = data != null ? data.array() : null;
            if (cache != null) {
                cache.set(CACHE_KEY, dataArr);
            }
        }
        return dataArr != null ? (dataArr.length >= BLOCK_SIZE ? dataArr : Arrays.copyOf(dataArr,
                BLOCK_SIZE)) : null;
    }

    /**
     * Write a file's block data to storage.
     * 
     * @param fileInfo
     * @param blockNum
     * @param data
     */
    private void writeFileBlock(FileInfo fileInfo, int blockNum, byte[] data) {
        Session session = getSession();
        CqlUtils.execute(session, CQL_WRITE_FILEDATA, consistencyLevelWriteFileData,
                ByteBuffer.wrap(data), fileInfo.id(), blockNum);
        ICache cache = getCache();
        if (cache != null) {
            final String CACHE_KEY = cacheKeyDataBlock(fileInfo, blockNum);
            cache.set(CACHE_KEY, data);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("writeFileBlock(" + fileInfo.name() + " - " + fileInfo.id() + "/"
                        + blockNum + ") --> update cache!");
            }
        }
    }

    /**
     * Gets a file's metadata info.
     * 
     * @param filename
     * @return
     */
    private FileInfo getFileInfo(String filename) {
        ICache cache = getCache();
        final String CACHE_KEY = cacheKeyFileInfo(filename);
        FileInfo fileInfo = (FileInfo) (cache != null ? cache.get(CACHE_KEY) : null);
        if (fileInfo == null) {
            Session session = getSession();
            Row row = CqlUtils.executeOne(session, CQL_GET_FILEINFO, consistencyLevelReadFileInfo,
                    filename);
            if (row != null) {
                fileInfo = FileInfo.newInstance(row);
                if (cache != null) {
                    cache.set(CACHE_KEY, fileInfo);
                }
            }
        }
        return fileInfo;
    }

    @SuppressWarnings("unchecked")
    private FileInfo[] getAllFileInfo() {
        if (LOGGER.isTraceEnabled()) {
            final String logMsg = "getAllFileInfo() is called";
            LOGGER.trace(logMsg);
        }
        ICache cache = getCache();
        List<FileInfo> result = (List<FileInfo>) (cache != null ? cache.get(cacheKeyAllFiles)
                : null);
        if (result == null) {
            Session session = getSession();
            ResultSet rs = CqlUtils.execute(session, CQL_GET_ALL_FILES,
                    consistencyLevelReadFileInfo);
            List<Row> allRows = rs != null ? rs.all() : new ArrayList<Row>();
            result = new ArrayList<FileInfo>();
            for (Row row : allRows) {
                FileInfo fileInfo = getFileInfo(row.getString(COL_FILE_NAME));
                if (fileInfo != null) {
                    result.add(fileInfo);
                }
            }
            if (cache != null) {
                cache.set(cacheKeyAllFiles, result);
            }
        }
        return result != null ? result.toArray(FileInfo.EMPTY_ARRAY) : FileInfo.EMPTY_ARRAY;
    }

    /**
     * Ensures a file's existence.
     * 
     * @param filename
     * @return
     */
    private FileInfo ensureFile(String filename) {
        if (LOGGER.isTraceEnabled()) {
            String logMsg = "ensureFile(" + filename + ") is called";
            LOGGER.trace(logMsg);
        }
        FileInfo fileInfo = FileInfo.newInstance(filename);
        Session session = getSession();
        CqlUtils.execute(session, CQL_ENSURE_FILE, consistencyLevelWriteFileInfo, fileInfo.id(),
                fileInfo.name());
        ICache cache = getCache();
        if (cache != null) {
            final String CACHE_KEY = cacheKeyFileInfo(fileInfo);
            cache.set(CACHE_KEY, fileInfo);
            cache.delete(cacheKeyAllFiles);
        }
        return getFileInfo(filename);
    }

    /**
     * Updates a file's metadata.
     * 
     * @param fileInfo
     * @return
     */
    private FileInfo updateFileInfo(FileInfo fileInfo) {
        if (LOGGER.isTraceEnabled()) {
            String logMsg = "updateFile(" + fileInfo.name() + "/" + fileInfo.id() + "/"
                    + fileInfo.size() + ") is called";
            LOGGER.trace(logMsg);
        }
        Session session = getSession();
        CqlUtils.execute(session, CQL_UPDATE_FILEINFO, consistencyLevelWriteFileInfo,
                fileInfo.size(), fileInfo.id(), fileInfo.name());
        ICache cache = getCache();
        if (cache != null) {
            final String CACHE_KEY = cacheKeyFileInfo(fileInfo);
            cache.set(CACHE_KEY, fileInfo);
            cache.delete(cacheKeyAllFiles);
        }
        return fileInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        destroy();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexOutput createOutput(String name, IOContext ioContext) throws IOException {
        FileInfo fileInfo = ensureFile(name);
        if (fileInfo == null) {
            throw new IOException("File [" + name + "] cannot be created!");
        }
        return new CassandraIndexOutput(fileInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexInput openInput(String name, IOContext ioContext) throws IOException {
        FileInfo fileInfo = getFileInfo(name);
        if (fileInfo == null) {
            throw new FileNotFoundException("File [" + name + "] not found!");
        }
        return new CassandraIndexInput(this, fileInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteFile(String name) throws IOException {
        FileInfo fileInfo = getFileInfo(name);
        if (fileInfo != null) {
            if (LOGGER.isTraceEnabled()) {
                final String logMsg = "deleteFile(" + name + "/" + fileInfo.id() + ") is called";
                LOGGER.trace(logMsg);
            }
            ICache cache = getCache();
            Session session = getSession();
            CqlUtils.execute(session, CQL_REMOVE_FILE, consistencyLevelRemoveFileInfo,
                    fileInfo.name());
            if (cache != null) {
                final String CACHE_KEY = cacheKeyFileInfo(fileInfo);
                cache.delete(CACHE_KEY);
                cache.delete(cacheKeyAllFiles);
            }
            long size = fileInfo.size();
            long numBlocks = (size / BLOCK_SIZE) + (size % BLOCK_SIZE != 0 ? 1 : 0);
            for (int i = 0; i < numBlocks; i++) {
                CqlUtils.execute(session, CQL_REMOVE_FILEDATA, consistencyLevelRemoveFileData,
                        fileInfo.id(), i);
                if (cache != null) {
                    final String CACHE_KEY = cacheKeyDataBlock(fileInfo, i);
                    cache.delete(CACHE_KEY);
                }
            }
        } else {
            if (LOGGER.isTraceEnabled()) {
                final String logMsg = "deleteFile(" + name + ") is called, but file is not found";
                LOGGER.trace(logMsg);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long fileLength(String name) throws IOException {
        FileInfo fileInfo = getFileInfo(name);
        if (fileInfo == null) {
            throw new FileNotFoundException("File [" + name + "] not found!");
        }
        return fileInfo.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] listAll() throws IOException {
        List<String> result = new ArrayList<String>();
        for (FileInfo fileInfo : getAllFileInfo()) {
            result.add(fileInfo.name());
        }
        return result.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void renameFile(String oldName, String newName) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            final String logMsg = "rename(" + oldName + "," + newName + ") is called";
            LOGGER.trace(logMsg);
        }

        FileInfo fileInfo = getFileInfo(oldName);
        if (fileInfo == null) {
            throw new IOException("File [" + oldName + "] not found!");
        }
        updateFileInfo(fileInfo.name(newName));

        Session session = getSession();
        CqlUtils.execute(session, CQL_REMOVE_FILE, consistencyLevelRemoveFileInfo, oldName);
        ICache cache = getCache();
        if (cache != null) {
            final String CACHE_KEY = cacheKeyFileInfo(oldName);
            cache.delete(CACHE_KEY);
            cache.delete(cacheKeyAllFiles);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sync(Collection<String> names) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            final String logMsg = "sync(" + names + ") is called";
            LOGGER.trace(logMsg);
        }
    }

    /*----------------------------------------------------------------------*/
    public CassandraLock createLock(String lockName) {
        return new CassandraLock(lockName);
    }

    /**
     * Cassandra implementation of {@link Lock}.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.1.0
     */
    private class CassandraLock extends Lock {

        private FileInfo fileInfo;

        public CassandraLock(String fileName) {
            fileInfo = FileInfo.newInstance(fileName);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean obtain() throws IOException {
            Session session = getSession();
            CqlUtils.execute(session, CQL_LOCK, consistencyLevelLock, fileInfo.name(),
                    fileInfo.id());
            FileInfo lockFile = getFileInfo(fileInfo.name());
            return lockFile != null && StringUtils.equals(lockFile.id(), fileInfo.id());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            deleteFile(fileInfo.name());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isLocked() throws IOException {
            return getFileInfo(fileInfo.name()) != null;
        }
    }

    /*----------------------------------------------------------------------*/
    /**
     * Cassandra implementation of {@link IndexOutput}.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.1.0
     */
    public class CassandraIndexOutput extends IndexOutput {

        private final Logger LOGGER = LoggerFactory.getLogger(CassandraIndexOutput.class);

        private CRC32 crc = new CRC32();
        private long bytesWritten = 0L;
        private FileInfo fileInfo;

        private int bufferOffset = 0;
        private int blockNum = 0;
        private byte[] buffer = new byte[CassandraDirectory.BLOCK_SIZE];

        public CassandraIndexOutput(FileInfo fileInfo) {
            super(fileInfo.name());
            this.fileInfo = fileInfo;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            flushBlock();
        }

        synchronized private void flushBlock() {
            if (bufferOffset > 0) {
                long t1 = System.currentTimeMillis();
                writeFileBlock(fileInfo, blockNum, buffer);
                blockNum++;
                bufferOffset = 0;
                buffer = new byte[BLOCK_SIZE];
                fileInfo.size(bytesWritten);
                updateFileInfo(fileInfo);
                long t2 = System.currentTimeMillis();
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("flushBlock[" + fileInfo.name() + "," + (blockNum - 1) + ","
                            + fileInfo.id() + "] in " + (t2 - t1) + " ms");
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeByte(byte b) throws IOException {
            crc.update(b);
            buffer[bufferOffset++] = b;
            bytesWritten++;
            fileInfo.size(bytesWritten);
            if (bufferOffset >= CassandraDirectory.BLOCK_SIZE) {
                flushBlock();
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < length; i++) {
                writeByte(b[offset + i]);
            }
            long t2 = System.currentTimeMillis();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("writeBytes[" + fileInfo.name() + "/" + offset + "/" + length
                        + "] in " + (t2 - t1) + " ms");
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getChecksum() throws IOException {
            return crc.getValue();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getFilePointer() {
            return bytesWritten;
        }
    }

    /*----------------------------------------------------------------------*/
    /**
     * Cassandra implementation of {@link IndexInput}.
     * 
     * @author Thanh Nguyen
     * @since 0.1.0
     */
    public class CassandraIndexInput extends IndexInput {

        private final Logger LOGGER = LoggerFactory.getLogger(CassandraIndexInput.class);

        private CassandraDirectory cassDir;
        private FileInfo fileInfo;

        private boolean isSlice = false;
        private byte[] block;
        private int blockOffset = 0;
        private int blockNum = 0;

        private long offset, end, pos;

        public CassandraIndexInput(CassandraDirectory cassDir, FileInfo fileInfo) {
            super(fileInfo.name());
            this.cassDir = cassDir;
            this.fileInfo = fileInfo;
            this.offset = 0L;
            this.pos = 0L;
            this.end = fileInfo.size();
        }

        public CassandraIndexInput(String resourceDesc, CassandraIndexInput another, long offset,
                long length) throws IOException {
            super(resourceDesc);
            this.cassDir = another.cassDir;
            this.fileInfo = another.fileInfo;
            this.offset = another.offset + offset;
            this.end = this.offset + length;
            this.blockNum = another.blockNum;
            this.blockOffset = another.blockOffset;
            // if (another.block != null) {
            // this.block = Arrays.copyOf(another.block, another.block.length);
            // }
            seek(0);
        }

        private void loadBlock(int blockNum) {
            if (LOGGER.isTraceEnabled()) {
                final String logMsg = "loadBlock(" + fileInfo.name() + "/" + blockNum + ")";
                LOGGER.trace(logMsg);
            }
            block = cassDir.readFileBlock(fileInfo, blockNum);
            this.blockNum = blockNum;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public CassandraIndexInput clone() {
            CassandraIndexInput clone = (CassandraIndexInput) super.clone();
            clone.cassDir = cassDir;
            clone.fileInfo = fileInfo;
            clone.offset = offset;
            clone.pos = pos;
            clone.end = end;
            clone.blockNum = blockNum;
            clone.blockOffset = blockOffset;
            if (block != null) {
                clone.block = Arrays.copyOf(block, block.length);
            }
            clone.isSlice = this.isSlice;
            return clone;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            // EMPTY
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getFilePointer() {
            return pos;
            // return pos + offset;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long length() {
            return end - offset;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void seek(long pos) throws IOException {
            if (pos < 0 || pos + offset > end) {
                throw new IllegalArgumentException("Seek position is out of range [0," + length()
                        + "]!");
            }

            if (LOGGER.isTraceEnabled()) {
                String logMsg = "seek(" + fileInfo.name() + "," + isSlice + "," + offset + "/"
                        + end + "," + pos + ") is called";
                LOGGER.trace(logMsg);
            }

            this.pos = pos;
            long newBlockNum = (pos + offset) / CassandraDirectory.BLOCK_SIZE;
            if (newBlockNum != blockNum) {
                loadBlock((int) newBlockNum);
            }
            blockOffset = (int) ((pos + offset) % CassandraDirectory.BLOCK_SIZE);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public IndexInput slice(String sliceDescription, long offset, long length)
                throws IOException {
            if (LOGGER.isTraceEnabled()) {
                final String logMsg = "slice(" + sliceDescription + "," + offset + "," + length
                        + ") -> " + fileInfo.name();
                LOGGER.trace(logMsg);
            }
            if (offset < 0 || length < 0 || offset + length > this.length()) {
                throw new IllegalArgumentException("slice(" + sliceDescription + ") "
                        + " out of bounds: " + this);
            }
            CassandraIndexInput clone = new CassandraIndexInput(sliceDescription, this, offset,
                    length);
            clone.isSlice = true;
            return clone;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public byte readByte() throws IOException {
            if (pos + offset >= end) {
                return -1;
            }

            if (block == null) {
                loadBlock(blockNum);
            }

            byte data = block[blockOffset++];
            pos++;
            if (blockOffset >= CassandraDirectory.BLOCK_SIZE) {
                loadBlock(blockNum + 1);
            }
            blockOffset = (int) ((pos + offset) % CassandraDirectory.BLOCK_SIZE);
            return data;
        }

        @Override
        public void readBytes(byte[] buffer, int offset, int length) throws IOException {
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < length; i++) {
                buffer[offset + i] = readByte();
            }
            long t2 = System.currentTimeMillis();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("readBytes[" + fileInfo.name() + "/" + offset + "/" + length + "] in "
                        + (t2 - t1) + " ms");
            }
        }

    }
}
