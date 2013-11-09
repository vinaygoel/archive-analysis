#!/usr/bin/env bash
# Author: vinay

# Runs a Job to generate CDX files for WARC files stored in a local directory.

if [ $# != 2 ] ; then
    echo "Usage: TOOL <LOCALWARCDIR> <LOCALCDXDIR>"
    echo "LOCALWARCDIR: LOCAL directory location containing WARC files"
    echo "LOCALCDXDIR: LOCAL directory location for the resulting CDX files"
    exit 1
fi

LOCALWARCDIR=$1
LOCALCDXDIR=$2

PROJECTDIR=`pwd`
IAHADOOPTOOLS=$PROJECTDIR/lib/ia-hadoop-tools-jar-with-dependencies.jar

mkdir -p $LOCALCDXDIR
if [ $? -ne 0 ]; then
        echo "Unable to create $LOCALCDXDIR"
        exit 2
fi

cd $LOCALWARCDIR

#replace exit statements with continue if you want Job to proceed despite some failures
ls | grep warc.gz$ | while read warcfile; do 
	warcbase=${warcfile%%.gz}
	java -Xmx2048m -jar $IAHADOOPTOOLS extractor -cdx $warcbase.gz > $LOCALCDXDIR/$warcbase.cdx;
	cdxstatus=$?
	if [ $cdxstatus -ne 0 ]; then
                rm -f $LOCALCDXDIR/$warcbase.cdx;
                echo "$warcbase cdx-gen-fail $cdxstatus"
                exit 3
        fi

	gzip $LOCALCDXDIR/$warcbase.cdx;
	cdxstatus=$?
	if [ $cdxstatus -ne 0 ]; then
                rm -f $LOCALCDXDIR/$warcbase.cdx.gz;
                echo "$warcbase cdx-gz-fail $cdxstatus"
                exit 4
        fi
	echo "$warcbase success 0";
done
echo "Job complete!"
