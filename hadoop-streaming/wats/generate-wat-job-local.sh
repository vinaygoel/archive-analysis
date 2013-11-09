#!/usr/bin/env bash
# Author: vinay

# Runs a Job to generate WAT files for WARC files stored in a local directory.

if [ $# != 2 ] ; then
    echo "Usage: TOOL <LOCALWARCDIR> <LOCALWATDIR>"
    echo "LOCALWARCDIR: LOCAL directory location containing WARC files"
    echo "LOCALWATDIR: LOCAL directory location for the resulting WAT files"
    exit 1
fi

LOCALWARCDIR=$1
LOCALWATDIR=$2

PROJECTDIR=`pwd`
IAHADOOPTOOLS=$PROJECTDIR/lib/ia-hadoop-tools-jar-with-dependencies.jar

mkdir -p $LOCALWATDIR
if [ $? -ne 0 ]; then
        echo "Unable to create $LOCALWATDIR"
        exit 2
fi

cd $LOCALWARCDIR

#replace exit statements with continue if you want Job to proceed despite some failures
ls | grep warc.gz$ | while read warcfile; do 
	warcbase=${warcfile%%.gz}
	java -Xmx2048m -jar $IAHADOOPTOOLS extractor -wat $warcbase.gz > $LOCALWATDIR/$warcbase.wat.gz;
	watstatus=$?
	if [ $watstatus -ne 0 ]; then
                rm -f $LOCALWATDIR/$warcbase.wat.gz;
                echo "$warcbase wat-gen-fail $watstatus"
                exit 3
        fi
	echo "$warcbase success 0";
done
echo "Job complete!"
