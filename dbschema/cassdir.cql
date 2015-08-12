DROP TABLE directory_metadata;
DROP TABLE file_data;

-- table to store directory's metadata (i.e. file information such as name, size, id, etc)
CREATE TABLE directory_metadata (
    filename            VARCHAR,
    filesize            BIGINT,
    fileid              VARCHAR,
    PRIMARY KEY (filename)
) WITH COMPACT STORAGE;

-- table to store actual file's data.
CREATE TABLE file_data (
    fileid              VARCHAR,
    blocknum            INT,
    blockdata           BLOB,
    PRIMARY KEY ((fileid, blocknum))
) WITH COMPACT STORAGE;
