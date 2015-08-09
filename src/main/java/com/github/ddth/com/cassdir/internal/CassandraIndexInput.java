package com.github.ddth.com.cassdir.internal;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.com.cassdir.CassandraDirectory;
import com.github.ddth.com.cassdir.FileInfo;

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
        if (LOGGER.isDebugEnabled()) {
            final String logMsg = "loadBlock(" + fileInfo.name() + "/" + blockNum + ")";
            LOGGER.debug(logMsg);
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

        if (LOGGER.isDebugEnabled()) {
            String logMsg = "seek(" + fileInfo.name() + "," + isSlice + "," + offset + "/" + end
                    + "," + pos + ") is called";
            LOGGER.debug(logMsg);
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
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            final String logMsg = "slice(" + sliceDescription + "," + offset + "," + length
                    + ") -> " + fileInfo.name();
            LOGGER.debug(logMsg);
        }
        if (offset < 0 || length < 0 || offset + length > this.length()) {
            throw new IllegalArgumentException("slice(" + sliceDescription + ") "
                    + " out of bounds: " + this);
        }
        CassandraIndexInput clone = new CassandraIndexInput(sliceDescription, this, offset, length);
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
        // if (LOGGER.isDebugEnabled()) {
        // LOGGER.debug("readBytes[" + length + "] in " + (t1 - t2) + " ms");
        // }
    }

}
