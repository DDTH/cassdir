package com.github.ddth.com.cassdir.internal;

import java.io.IOException;
import java.util.Arrays;
import java.util.zip.CRC32;

import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.com.cassdir.CassandraDirectory;
import com.github.ddth.com.cassdir.FileInfo;

/**
 * Cassandra implementation of {@link IndexOutput}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class CassandraIndexOutput extends IndexOutput {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraIndexOutput.class);

    private CassandraDirectory cassDir;

    private CRC32 crc = new CRC32();
    private long bytesWritten = 0L;
    private FileInfo fileInfo;

    private int bufferOffset = 0;
    private int blockNum = 0;
    private byte[] buffer = new byte[CassandraDirectory.BLOCK_SIZE];

    public CassandraIndexOutput(CassandraDirectory cassDir, FileInfo fileInfo) {
        super(fileInfo.name());
        this.cassDir = cassDir;
        this.fileInfo = fileInfo;

        Arrays.fill(buffer, (byte) 0);
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
            cassDir.writeFileBlock(fileInfo, blockNum, buffer);
            blockNum++;
            bufferOffset = 0;
            Arrays.fill(buffer, (byte) 0);

            fileInfo.size(bytesWritten);
            cassDir.updateFile(fileInfo);
            long t2 = System.currentTimeMillis();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("flushBlock[" + fileInfo.name() + "," + (blockNum - 1) + ","
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
        // if (LOGGER.isDebugEnabled()) {
        // LOGGER.debug("writeBytes[" + length + "] in " + (t1 - t2) + " ms");
        // }
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
