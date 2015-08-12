package com.github.ddth.com.cassdir.internal;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import com.github.ddth.com.cassdir.CassandraDirectory;

/**
 * Lock factory to be used with {@link CassandraDirectory}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class CassandraLockFactory extends LockFactory {

    public final static CassandraLockFactory INSTANCE = new CassandraLockFactory();

    private CassandraLockFactory() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Lock makeLock(Directory dir, String lockName) {
        if (!(dir instanceof CassandraDirectory)) {
            throw new IllegalArgumentException("Expect argument of type ["
                    + CassandraDirectory.class.getName() + "]!");
        }
        CassandraDirectory cassDir = (CassandraDirectory) dir;
        return cassDir.createLock(lockName);
    }

}
