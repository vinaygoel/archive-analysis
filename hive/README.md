Heritrix Crawl Log Table
-------------------------

The Crawl Log Table is defined as an external table in Hive.

```
hive > CREATE EXTERNAL TABLE crawllogtable                                                                                             
	> (ts  STRING,                                                                                                                   
	> response STRING,
	> size STRING,
	> url STRING,
	> hoppath STRING,
	> referrer STRING,
	> mime STRING,
	> thread STRING,
	> downloadtime STRING,
	> digest STRING,
	> sourcetag STRING,
	> crud STRING,
	> annotation STRING )
	> PARTITIONED BY 
	> (partner STRING,
	> col STRING,
	> instance STRING)
	> ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
	> WITH SERDEPROPERTIES (
	> "input.regex" = "([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+)",
	> "output.format.string" = "%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s %10$s %11$s %12$s %13$s")
	> STORED AS TEXTFILE;
```

To list all the tables defined:

```
hive > SHOW tables;
```

To list all the columns defined for crawllogtable:

```
hive > DESCRIBE crawllogtable;
```

Loading Data
------------

The external table crawllogtable points to specific locations in HDFS. The actual crawl log data is stored in HDFS as text files. 

Note that we have defined the table to be partitioned by partner, collection (col) and crawl-instance (instance). This allows the user to query all the crawl logs or efficiently query a subset of them as defined by the partitions (e.g. query only LOC-MONTHLY-033 crawl logs or query only LOC crawl logs etc.)

The HDFS locations for the crawl logs are organized as /crawl-logs/{partner}/{collection}/{instance}/. So, LOC-MONTHLY-033 crawl logs will be stored in /crawl-logs/loc/monthly/033/


Store log data into HDFS:

```
$ /home/webcrawl/hadoop/bin/hadoop fs -mkdir /crawl-logs/loc/monthly/033/
$ /home/webcrawl/hadoop/bin/hadoop fs -put crawl.log /crawl-logs/loc/monthly/033/
```

Define new partition in the crawllogtable (example: LOC-MONTHLY-033):

```
hive >ALTER TABLE crawllogtable ADD IF NOT EXISTS
	> PARTITION (
	> partner='loc',
	> col='monthly',
	> instance='033')
	> LOCATION '/crawl-logs/loc/monthly/033/';
```

Querying Data
-------------

Simple query example:

```
hive > SELECT mime,url from crawllogtable where response = "200" and col = "monthly";
```

Regex example:

```
hive > select max(cast(size as BIGINT)) from crawllogtable where partner="loc" and col="monthly" and instance="033" and mime rlike 'text/.*' and size < > "-";
```

'Like' query example:

```
hive > select avg(cast(size as BIGINT)) from crawllogtable where partner="loc" and col="monthly" and instance="033" and hoppath like '%RR' and size < > "-";
```

'Order-by' example:

```
hive > select cast(size AS BIGINT) as docsize,url from crawllogtable where partner="loc" and col="monthly" and instance="033" and size < > "-" order by docsize desc;
```

'Group-by' query example:

```
hive > select sum(cast(size AS BIGINT)) as total,mime from crawllogtable where partner="loc" and col="monthly" and instance="033" and size < > "-" group by mime;
```

Top Digests:

```
hive > select digest, count(1) as counts from crawllogtable where partner="loc" and col="monthly" and instance="033" group by digest order by counts desc limit 20;
```

Lookup example:

```
hive > select url from crawllogtable where partner="loc" and col="monthly" and instance="033" and digest="sha1:3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ" limit 20;
```

Total Captures-size per Host:

```
hive > select regexp_extract(url,'(https?:\/\/|dns:)?([^\/]+).*',2) as host, sum(cast(size AS BIGINT)) as total from crawllogtable where partner="loc" and col="monthly" and instance="033" and size < > "-" group by regexp_extract(url,'(https?:\/\/|dns:)?([^\/]+).*',2) order by total  desc limit 20;
```

Total Captures per Host:

```
hive > select regexp_extract(url,'(https?:\/\/|dns:)?([^\/]+).*',2) as host, count(1) as total from crawllogtable where partner="loc" and col="monthly" and instance="033" and size < > "-" group by regexp_extract(url,'(https?:\/\/|dns:)?([^\/]+).*',2) order by total  desc limit 20;
```

Store results of query in local file:

```
hive > insert overwrite local directory '/tmp/example4/' select url, hoppath from crawllogtable where partner="loc" and col="monthly" and instance="033" and hoppath like '%RR'  limit 10;
```

The default delimiter for the stored results file is Ctrl-A. So, to replace delimiter by a " ", run a simple sed command.

```
$ cat /tmp/example/000000_0  | sed 's/'`echo "\o001"`'/ /g'
```

Please refer to the Hive Tutorial for more: https://cwiki.apache.org/confluence/display/Hive/Tutorial
