package com.github.ddth.com.cassdir.qnd;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import com.github.ddth.com.cassdir.CassandraDirectory;
import com.github.ddth.com.cassdir.internal.CassandraIndexInput;
import com.github.ddth.com.cassdir.internal.CassandraIndexOutput;

public class BaseQndCassandraDir {

    public static final String CASS_HOSTSANDPORTS = "localhost:9042";
    public static final String CASS_USER = "tsc";
    public static final String CASS_PASSWORD = "tsc";
    public static final String CASS_KEYSPACE = "tsc_demo";

    public static void initLoggers(Level level) {
        {
            Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
            logger.setLevel(Level.ERROR);
        }
        {
            Logger logger = (Logger) LoggerFactory.getLogger(CassandraDirectory.class);
            logger.setLevel(level);
        }
        {
            Logger logger = (Logger) LoggerFactory.getLogger(CassandraIndexInput.class);
            logger.setLevel(level);
        }
        {
            Logger logger = (Logger) LoggerFactory.getLogger(CassandraIndexOutput.class);
            logger.setLevel(level);
        }
    }

}
