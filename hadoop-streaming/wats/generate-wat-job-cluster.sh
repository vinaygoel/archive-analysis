#!/usr/bin/env bash
# Author: vinay

# Runs a Hadoop Streaming Job to generate WAT files
# for WARC files stored in a HDFS directory.
# Finds the set of WARC files that do not have a corresponding WAT file
# and generates WAT files for this set

if [ $# != 4 ] ; then
    echo "Usage: TOOL <HDFSWARCDIR> <HDFSWATDIR> <HDFSWORKDIR> <LOCALWORKDIR>"
    echo "HDFSWARCDIR: HDFS directory location containing WARC files"
    echo "HDFSWATDIR: HDFS directory location for the resulting WAT files"
    echo "HDFSWORKDIR: HDFS directory location for scratch space (will be created if non-existent)"
    echo "LOCALWORKDIR: Local directory for scratch space (will be created if non-existent)"
    exit 1
fi

HDFSWARCDIR=$1
HDFSWATDIR=$2
HDFSWORKDIR=$3
LOCALWORKDIR=$4

HADOOPDIR=/home/webcrawl/hadoop-0.20.2-cdh3u3/
PROJECTDIR=`pwd`

JOBNAME=WAT-Generator
HADOOPCMD=$HADOOPDIR/bin/hadoop
HADOOPSTREAMJAR=$HADOOPDIR/contrib/streaming/hadoop-streaming-*.jar
TASKTIMEOUT=3600000

MAPPERFILE=$PROJECTDIR/hadoop-streaming/wats/generate-wat-mapper.sh
MAPPER=generate-wat-mapper.sh

#create HDFSWATDIR
$HADOOPCMD fs -mkdir $HDFSWATDIR 2> /dev/null

#create task dir in HDFS
UPDATENUM=`date +%s`
TASKDIR=$HDFSWORKDIR/$UPDATENUM
$HADOOPCMD fs -mkdir $TASKDIR

mkdir -p $LOCALWORKDIR
if [ $? -ne 0 ]; then
    echo "ERROR: unable to create $LOCALWORKDIR"
    exit 2
fi

#dump list of WARC files (only prefixes)
$HADOOPCMD fs -ls $HDFSWARCDIR | grep warc.gz$ | tr -s ' ' | cut -f8 -d ' ' | awk -F'/' '{ print $NF }' | sort | uniq | sed "s@.warc.gz@.warc@" > $LOCALWORKDIR/warcs.list 

#dump list of WAT files already generated (only prefixes)
$HADOOPCMD fs -ls $HDFSWATDIR | grep wat.gz$ | tr -s ' ' | cut -f8 -d ' ' | awk -F'/' '{ print $NF }' | sort | uniq | sed "s@.warc.wat.gz@.warc@"  > $LOCALWORKDIR/wats.list 

# find list of prefixes to be processed
join -v1 $LOCALWORKDIR/warcs.list $LOCALWORKDIR/wats.list > $LOCALWORKDIR/todo.list

# if todo.list is empty, exit
if [[ ! -s $LOCALWORKDIR/todo.list ]] ; then echo "No new WARCs to be processed"; rm -f $LOCALWORKDIR/warcs.list $LOCALWORKDIR/wats.list $LOCALWORKDIR/todo.list; exit 0; fi

#create task file from todo.list
cat $LOCALWORKDIR/todo.list | sed "s@\$@ $HDFSWARCDIR $HDFSWATDIR@" | $PROJECTDIR/bin/unique-sorted-lines-by-first-field.pl > $LOCALWORKDIR/taskfile

num=`wc -l $LOCALWORKDIR/taskfile | cut -f1 -d ' '`;
echo "Number of new WARCs to be processed - $num";

#store task file in HDFS
$HADOOPCMD fs -put $LOCALWORKDIR/taskfile $TASKDIR/taskfile

INPUT=$TASKDIR/taskfile
OUTPUT=$TASKDIR/result

echo "Starting Hadoop Streaming job to process $num WARCs";
# run streaming job - 1 mapper per file to be processed
$HADOOPCMD jar $HADOOPSTREAMJAR -D mapred.job.name=$JOBNAME -D mapred.reduce.tasks=0 -D mapred.task.timeout=$TASKTIMEOUT -D mapred.line.input.format.linespermap=1 -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat -input $INPUT -output $OUTPUT -mapper $MAPPER -file $MAPPERFILE

if [ $? -ne 0 ]; then
    echo "ERROR: streaming job failed! - $INPUT"
    rm -f $LOCALWORKDIR/warcs.list $LOCALWORKDIR/wats.list $LOCALWORKDIR/todo.list $LOCALWORKDIR/taskfile
    exit 3
fi

rm -f $LOCALWORKDIR/warcs.list $LOCALWORKDIR/wats.list $LOCALWORKDIR/todo.list $LOCALWORKDIR/taskfile
echo "WAT Generation Job complete - per file status in $OUTPUT";

