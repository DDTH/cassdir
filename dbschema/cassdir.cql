DROP TABLE directory_metadata;
DROP TABLE file_data;

CREATE TABLE directory_metadata (
    filename            VARCHAR,
    filesize            BIGINT,
    fileid              VARCHAR,
    PRIMARY KEY (filename)
) WITH COMPACT STORAGE;

CREATE TABLE file_data (
    fileid              VARCHAR,
    blocknum            INT,
    blockdata           BLOB,
    PRIMARY KEY ((fileid), blocknum)
) WITH COMPACT STORAGE;
