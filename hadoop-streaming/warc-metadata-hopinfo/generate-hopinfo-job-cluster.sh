#!/usr/bin/env bash
# Author: vinay

# Runs a Hadoop Streaming Job to generate HOPINFO files
# for WARC files stored in a HDFS directory.
# Finds the set of WARC files that do not have a corresponding HOPINFO file
# and generates HOPINFO files for this set

if [ $# != 4 ] ; then
    echo "Usage: TOOL <HDFSWARCDIR> <HDFSHOPINFODIR> <HDFSWORKDIR> <LOCALWORKDIR>"
    echo "HDFSWARCDIR: HDFS directory location containing WARC files"
    echo "HDFSHOPINFODIR: HDFS directory location for the resulting HOPINFO files"
    echo "HDFSWORKDIR: HDFS directory location for scratch space (will be created if non-existent)"
    echo "LOCALWORKDIR: Local directory for scratch space (will be created if non-existent)"
    exit 1
fi

HDFSWARCDIR=$1
HDFSHOPINFODIR=$2
HDFSWORKDIR=$3
LOCALWORKDIR=$4

#HADOOP_HOME=/home/webcrawl/hadoop-0.20.2-cdh3u3/
PROJECTDIR=`pwd`

JOBNAME=HOPINFO-Generator
HDFSCMD=$HADOOP_HOME/bin/hadoop
HADOOPSTREAMJAR=$HADOOP_HOME/contrib/streaming/hadoop-streaming-*.jar
TASKTIMEOUT=3600000

MAPPERFILE=$PROJECTDIR/hadoop-streaming/warc-metadata-hopinfo/generate-hopinfo-mapper.sh
MAPPER=generate-hopinfo-mapper.sh

#create HDFSHOPINFODIR
$HDFSCMD dfs -mkdir $HDFSHOPINFODIR 2> /dev/null

#create task dir in HDFS
UPDATENUM=`date +%s`
TASKDIR=$HDFSWORKDIR/$UPDATENUM
$HDFSCMD dfs -mkdir $TASKDIR

mkdir -p $LOCALWORKDIR
if [ $? -ne 0 ]; then
    echo "ERROR: unable to create $LOCALWORKDIR"
    exit 2
fi

#dump list of WARC files (only prefixes)
$HDFSCMD dfs -ls $HDFSWARCDIR | grep warc.gz$ | tr -s ' ' | cut -f8 -d ' ' | awk -F'/' '{ print $NF }' | sort | uniq | sed "s@.warc.gz@.warc@" > $LOCALWORKDIR/warcs.list 

#dump list of HOPINFO files already generated (only prefixes)
$HDFSCMD dfs -ls $HDFSHOPINFODIR | grep hopinfo.gz$ | tr -s ' ' | cut -f8 -d ' ' | awk -F'/' '{ print $NF }' | sort | uniq | sed "s@.warc.hopinfo.gz@.warc@"  > $LOCALWORKDIR/hopinfos.list 

# find list of prefixes to be processed
join -v1 $LOCALWORKDIR/warcs.list $LOCALWORKDIR/hopinfos.list > $LOCALWORKDIR/todo.list

# if todo.list is empty, exit
if [[ ! -s $LOCALWORKDIR/todo.list ]] ; then echo "No new WARCs to be processed"; rm -f $LOCALWORKDIR/warcs.list $LOCALWORKDIR/hopinfos.list $LOCALWORKDIR/todo.list; exit 0; fi

#create task file from todo.list
cat $LOCALWORKDIR/todo.list | sed "s@\$@ $HDFSWARCDIR $HDFSHOPINFODIR@" | $PROJECTDIR/bin/unique-sorted-lines-by-first-field.pl > $LOCALWORKDIR/taskfile

num=`wc -l $LOCALWORKDIR/taskfile | cut -f1 -d ' '`;
echo "Number of new WARCs to be processed - $num";

#store task file in HDFS
$HDFSCMD dfs -put $LOCALWORKDIR/taskfile $TASKDIR/taskfile

INPUT=$TASKDIR/taskfile
OUTPUT=$TASKDIR/result

echo "Starting Hadoop Streaming job to process $num WARCs";
# run streaming job - 1 mapper per file to be processed
$HADOOPCMD jar $HADOOPSTREAMJAR -D mapred.job.name=$JOBNAME -D mapred.reduce.tasks=0 -D mapred.task.timeout=$TASKTIMEOUT -D mapred.line.input.format.linespermap=1 -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat -input $INPUT -output $OUTPUT -mapper $MAPPER -file $MAPPERFILE

if [ $? -ne 0 ]; then
    echo "ERROR: streaming job failed! - $INPUT"
    rm -f $LOCALWORKDIR/warcs.list $LOCALWORKDIR/hopinfos.list $LOCALWORKDIR/todo.list $LOCALWORKDIR/taskfile
    exit 3
fi

rm -f $LOCALWORKDIR/warcs.list $LOCALWORKDIR/hopinfos.list $LOCALWORKDIR/todo.list $LOCALWORKDIR/taskfile
echo "HOPINFO Generation Job complete - per file status in $OUTPUT";

