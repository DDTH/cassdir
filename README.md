cassdir
=======

[Cassandra](http://cassandra.apache.org) implementation of [Lucene](http://lucene.apache.org) Directory.

By Thanh Ba Nguyen (btnguyen2k (at) gmail.com)

Project home:
[https://github.com/DDTH/cassdir](https://github.com/DDTH/cassdir)


## Features ##

- Store [Lucene](http://lucene.apache.org)'s index in [Cassandra](http://cassandra.apache.org).
- Performance enhancement with caching support.
- Support fine-tuning Cassandra consistency level per operation (read/write file data, lock, remove file, etc).


## Installation ##

Latest release version: `0.1.0`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency (available soon):

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>cassdir</artifactId>
	<version>0.1.0</version>
</dependency>
```

### Related projects/libs ###

- [ddth-cache-adapter](https://github.com/DDTH/ddth-cache-adapter): for caching support.
- [ddth-redis](https://github.com/DDTH/ddth-redis): to use Redis as cache backend.
- [ddth-cql-utils](https://github.com/DDTH/ddth-cql-utils): library to interact with Cassandra via CQL.
- [Datastax Java Driver for Cassandra](https://github.com/datastax/java-driver): Java driver to access Cassandra cluster.


## Usage ##

Cassandra column family schema (as CQL): see [dbschema/cassdir.cql](dbschema/cassdir.cql).

Create a `CassandraDirectory` instance:
```java
String cassHostsAndPorts = "localhost:9042,host2:port2,host3:port3";
CassandraDirectory DIR = new CassandraDirectory(cassHostsAndPorts, cassUser, cassPassword, cassKeySpace);
DIR.init();
```

Index documents with `IndexWriter`:
```java
Analyzer analyzer = new StandardAnalyzer();
IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
IndexWriter iw = new IndexWriter(DIR, iwc);

// add/update documents
// ...

iw.commit();
iw.close();
```

Or, search documents with `IndexSearcher`:
```java
IndexReader ir = DirectoryReader.open(DIR);
IndexSearcher is = new IndexSearcher(ir);

// search documents
// ...

ir.close();
```

Call `CassandraDirectory.destroy()` when done.


Examples: see [src/test/java](src/test/java).

## License ##

See LICENSE.txt for details. Copyright (c) 2015 Thanh Ba Nguyen.

Third party libraries are distributed under their own license(s).
