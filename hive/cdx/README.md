CDX Table
-------------------------

The CDX Table is defined as an external table in Hive.

```
hive > CREATE EXTERNAL TABLE cdxtable   
     > (url STRING,
     > ts STRING,
     > origurl STRING,
     > mime STRING,
     > rescode STRING,
     > checksum STRING,
     > redirecturl STRING,
     > meta STRING,
     > compressedcompressedsize STRING,
     > offset STRING,
     > filename STRING)
     > PARTITIONED BY 
     > (partner STRING,
     > col STRING,
     > instance STRING)
     > ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
     > STORED AS TEXTFILE;

```

To list all the tables defined:

```
hive > SHOW tables;
```

To list all the columns defined for cdxtable:

```
hive > DESCRIBE cdxtable;
```

Loading Data
------------

The external table cdxtable points to specific locations in HDFS. The actual CDX data is stored in HDFS as GZIP compressed text files. 

Note that we have defined the table to be partitioned by partner, collection (col) and crawl-instance (instance). This allows the user to query all CDX files or efficiently query a subset of them as defined by the partitions (e.g. query only LOC-MONTHLY-033 CDX files or query only LOC CDX files etc.)

The HDFS locations for the CDX files are organized as /cdx/{partner}/{collection}/{instance}/. So, LOC-MONTHLY-033 CDX files will be stored in /cdx/loc/monthly/033/


Store CDX data into HDFS:

```
$ /home/webcrawl/hadoop/bin/hadoop fs -mkdir /cdx/loc/monthly/033/
$ /home/webcrawl/hadoop/bin/hadoop fs -put LOC-MONTHLY*.cdx.gz /cdx/loc/monthly/033/
```

Define new partition in the cdxtable (example: LOC-MONTHLY-033):

```
hive >ALTER TABLE cdxtable ADD IF NOT EXISTS
	> PARTITION (
	> partner='loc',
	> col='monthly',
	> instance='033')
	> LOCATION '/cdx/loc/monthly/033/';

```

Querying Data
-------------

Simple query example:

```
hive > SELECT mime,url from cdxtable where rescode = "200" and col = "monthly";
```

Regex and 'like' query example:

```
hive > select max(cast(compressedsize as BIGINT)) from cdxtable where partner="loc" and col="monthly" and instance="033" and mime rlike 'text/.*' and compressedsize < > "-";
```

'Order-by' example:

```
hive > select cast(compressedsize AS BIGINT) as doccompressedsize,url from cdxtable where partner="loc" and col="monthly" and instance="033" and compressedsize < > "-" order by doccompressedsize desc;
```

'Group-by' query example:

```
hive > select sum(cast(compressedsize AS BIGINT)) as total,mime from cdxtable where partner="loc" and col="monthly" and instance="033" and compressedsize < > "-" group by mime;
```

Top Digests:

```
hive > select checksum, count(1) as counts from cdxtable where partner="loc" and col="monthly" and instance="033" group by checksum order by counts desc limit 20;
```

Lookup example:

```
hive > select url from cdxtable where partner="loc" and col="monthly" and instance="033" and checksum="3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ" limit 20;
```

Total Captures-compressedsize per Host:

```
hive > select regexp_extract(origurl,'(https?:\/\/|dns:)?([^\/]+).*',2) as host, sum(cast(compressedsize AS BIGINT)) as total from cdxtable where partner="loc" and col="monthly" and instance="033" and compressedsize < > "-" group by regexp_extract(origurl,'(https?:\/\/|dns:)?([^\/]+).*',2) order by total  desc limit 20;
```

Total Captures per Host:

```
hive > select regexp_extract(origurl,'(https?:\/\/|dns:)?([^\/]+).*',2) as host, count(1) as total from cdxtable where partner="loc" and col="monthly" and instance="033" and compressedsize < > "-" group by regexp_extract(origurl,'(https?:\/\/|dns:)?([^\/]+).*',2) order by total  desc limit 20;
```

Store results of query in local file:

```
hive > insert overwrite local directory '/tmp/example/' select url,rescode from cdxtable where partner="loc" and col="monthly" and instance="033" and rescode = "302"  limit 10;
```

The default delimiter for the stored results file is Ctrl-A. So, to replace delimiter by a " ", run a simple sed command.

```
$ cat /tmp/example/000000_0  | sed 's/'`echo "\o001"`'/ /g'
```

Please refer to the Hive Tutorial for more: https://cwiki.apache.org/confluence/display/Hive/Tutorial
