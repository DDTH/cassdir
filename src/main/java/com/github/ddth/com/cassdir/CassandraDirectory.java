package com.github.ddth.com.cassdir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.ddth.com.cassdir.internal.CassandraIndexInput;
import com.github.ddth.com.cassdir.internal.CassandraIndexOutput;
import com.github.ddth.cql.CqlUtils;
import com.github.ddth.cql.SessionManager;

/**
 * Cassandra implementation of {@link Directory}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class CassandraDirectory extends BaseDirectory {

    public Set<String> files = new HashSet<String>();

    public final static int BLOCK_SIZE = 64 * 1024; // 64Kb
    private final static String DEFAULT_TBL_METADATA = "directory_metadata";
    private final static String DEFAULT_TBL_FILEDATA = "file_data";

    private String tableFiledata = DEFAULT_TBL_FILEDATA;
    private String tableMetadata = DEFAULT_TBL_METADATA;
    private final static String COL_FILE_NAME = "filename";
    private final static String COL_FILE_SIZE = "filesize";
    private final static String COL_FILE_ID = "fileid";
    private final static String COL_BLOCK_NUM = "blocknum";
    private final static String COL_BLOCK_DATA = "blockdata";

    private String CQL_REMOVE_FILE = "DELETE FROM {0} WHERE " + COL_FILE_NAME + "=?";
    private String CQL_REMOVE_FILEDATA = "DELETE FROM {0} WHERE " + COL_FILE_ID + "=?";

    private String CQL_LOAD_FILEDATA = "SELECT "
            + StringUtils.join(new String[] { COL_FILE_ID, COL_BLOCK_NUM, COL_BLOCK_DATA }, ",")
            + " FROM {0} WHERE " + COL_FILE_ID + "=? AND " + COL_BLOCK_NUM + "=?";
    private String CQL_WRITE_FILEDATA = "UPDATE {0} SET " + COL_BLOCK_DATA + "=? WHERE "
            + COL_FILE_ID + "=? AND " + COL_BLOCK_NUM + "=?";

    private String CQL_GET_FILEINFO = "SELECT "
            + StringUtils.join(new String[] { COL_FILE_NAME, COL_FILE_SIZE, COL_FILE_ID }, ",")
            + " FROM {0} WHERE " + COL_FILE_NAME + "=?";
    private String CQL_GET_ALL_FILES = "SELECT "
            + StringUtils.join(new String[] { COL_FILE_NAME, COL_FILE_SIZE, COL_FILE_ID }, ",")
            + " FROM {0}";
    private String CQL_ENSURE_FILE = "UPDATE {0} SET " + COL_FILE_ID + "=? WHERE " + COL_FILE_NAME
            + "=?";
    private String CQL_UPDATE_FILE = "UPDATE {0} SET " + COL_FILE_SIZE + "=?," + COL_FILE_ID
            + "=? WHERE " + COL_FILE_NAME + "=?";

    private Logger LOGGER = LoggerFactory.getLogger(CassandraDirectory.class);

    private String cassandraHostsAndPorts;
    private String cassandraKeyspace;
    private String cassandraUser, cassandraPassword;
    private SessionManager sessionManager;

    public CassandraDirectory(String cassandraHostsAndPorts, String cassandraUser,
            String cassandraPassword, String cassandraKeyspace) {
        super(NoLockFactory.INSTANCE);
        this.cassandraHostsAndPorts = cassandraHostsAndPorts;
        this.cassandraKeyspace = cassandraKeyspace;
        this.cassandraUser = cassandraUser;
        this.cassandraPassword = cassandraPassword;
        init();
    }

    public void init() {
        if (sessionManager == null) {
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
        CQL_UPDATE_FILE = MessageFormat.format(CQL_UPDATE_FILE, tableMetadata);
    }

    public void destroy() {
        if (sessionManager != null) {
            sessionManager.destroy();
            sessionManager = null;
        }
    }

    private Session getSession() {
        return sessionManager.getSession(cassandraHostsAndPorts, cassandraUser, cassandraPassword,
                cassandraKeyspace);
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
    public byte[] readFileBlock(FileInfo fileInfo, int blockNum) {
        Session session = getSession();
        Row row = CqlUtils.executeOne(session, CQL_LOAD_FILEDATA, ConsistencyLevel.LOCAL_QUORUM,
                fileInfo.id(), blockNum);
        ByteBuffer data = row != null ? row.getBytes(COL_BLOCK_DATA) : null;
        byte[] dataArr = data != null ? data.array() : null;
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
    public void writeFileBlock(FileInfo fileInfo, int blockNum, byte[] data) {
        Session session = getSession();
        CqlUtils.execute(session, CQL_WRITE_FILEDATA, ConsistencyLevel.LOCAL_QUORUM,
                ByteBuffer.wrap(data), fileInfo.id(), blockNum);
    }

    private FileInfo getFileInfo(String filename) {
        Session session = getSession();
        Row row = CqlUtils.executeOne(session, CQL_GET_FILEINFO, ConsistencyLevel.LOCAL_QUORUM,
                filename);
        if (row != null) {
            FileInfo fileInfo = FileInfo.newInstance(row);
            files.add(fileInfo.name());
            return fileInfo;
        }
        return null;
    }

    private FileInfo[] getAllFileInfo() {
        Session session = getSession();
        ResultSet rs = CqlUtils.execute(session, CQL_GET_ALL_FILES, ConsistencyLevel.LOCAL_QUORUM);
        List<Row> allRows = rs != null ? rs.all() : new ArrayList<Row>();
        List<FileInfo> result = new ArrayList<FileInfo>();
        for (Row row : allRows) {
            FileInfo fileInfo = FileInfo.newInstance(row);
            result.add(fileInfo);
            files.add(fileInfo.name());
        }
        return result.toArray(FileInfo.EMPTY_ARRAY);
    }

    private FileInfo ensureFile(String filename) {
        if (LOGGER.isDebugEnabled()) {
            String logMsg = "ensureFile(" + filename + ") is called";
            LOGGER.debug(logMsg);
        }
        FileInfo fileInfo = FileInfo.newInstance(filename);
        Session session = getSession();
        CqlUtils.execute(session, CQL_ENSURE_FILE, ConsistencyLevel.LOCAL_QUORUM, fileInfo.id(),
                fileInfo.name());
        return getFileInfo(filename);
    }

    public FileInfo updateFile(FileInfo fileInfo) {
        if (LOGGER.isDebugEnabled()) {
            String logMsg = "updateFile(" + fileInfo.name() + "/" + fileInfo.id() + "/"
                    + fileInfo.size() + ") is called";
            LOGGER.debug(logMsg);
        }
        Session session = getSession();
        CqlUtils.execute(session, CQL_UPDATE_FILE, ConsistencyLevel.LOCAL_QUORUM, fileInfo.size(),
                fileInfo.id(), fileInfo.name());
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
            throw new IOException("File [" + name + "] not found!");
        }
        return new CassandraIndexOutput(this, fileInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexInput openInput(String name, IOContext ioContext) throws IOException {
        FileInfo fileInfo = getFileInfo(name);
        if (fileInfo == null) {
            throw new IOException("File [" + name + "] not found!");
        }
        return new CassandraIndexInput(this, fileInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteFile(String name) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            final String logMsg = "deleteFile(" + name + ") is called";
            LOGGER.debug(logMsg);
        }
        FileInfo fileInfo = getFileInfo(name);
        if (fileInfo != null) {
            files.remove(fileInfo.name());
            Session session = getSession();
            CqlUtils.execute(session, CQL_REMOVE_FILE, ConsistencyLevel.LOCAL_QUORUM,
                    fileInfo.name());
            CqlUtils.execute(session, CQL_REMOVE_FILEDATA, ConsistencyLevel.LOCAL_QUORUM,
                    fileInfo.id());
        } else {
            throw new IllegalStateException("File [" + name + "] not found!");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long fileLength(String name) throws IOException {
        FileInfo fileInfo = getFileInfo(name);
        if (fileInfo == null) {
            throw new IOException("File [" + name + "] not found!");
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
        if (LOGGER.isDebugEnabled()) {
            final String logMsg = "rename(" + oldName + "," + newName + ") is called";
            LOGGER.debug(logMsg);
        }

        FileInfo fileInfo = getFileInfo(oldName);
        if (fileInfo == null) {
            throw new IOException("File [" + oldName + "] not found!");
        }
        updateFile(fileInfo.name(newName));

        Session session = getSession();
        CqlUtils.execute(session, CQL_REMOVE_FILE, ConsistencyLevel.ONE, oldName);
        files.remove(oldName);
        files.add(newName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sync(Collection<String> names) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            final String logMsg = "sync(" + names + ") is called";
            LOGGER.debug(logMsg);
        }
    }

    /*----------------------------------------------------------------------*/
    // private final class CassandraLockFactory extends LockFactory {
    //
    // @Override
    // public Lock makeLock(Directory arg0, String arg1) {
    // // TODO Auto-generated method stub
    // return null;
    // }
    //
    // }
}
