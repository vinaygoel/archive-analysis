#!/usr/bin/env bash
# Author: vinay

# Runs a Job to generate CDX files for ARC files stored in a local directory.

if [ $# != 2 ] ; then
    echo "Usage: TOOL <LOCALARCDIR> <LOCALCDXDIR>"
    echo "LOCALARCDIR: LOCAL directory location containing ARC files"
    echo "LOCALCDXDIR: LOCAL directory location for the resulting CDX files"
    exit 1
fi

LOCALARCDIR=$1
LOCALCDXDIR=$2

PROJECTDIR=`pwd`
IAHADOOPTOOLS=$PROJECTDIR/lib/ia-hadoop-tools-jar-with-dependencies.jar

mkdir -p $LOCALCDXDIR
if [ $? -ne 0 ]; then
        echo "Unable to create $LOCALCDXDIR"
        exit 2
fi

cd $LOCALARCDIR

#replace exit statements with continue if you want Job to proceed despite some failures
ls | grep arc.gz$ | while read arcfile; do 
	arcbase=${arcfile%%.gz}
	java -Xmx2048m -jar $IAHADOOPTOOLS extractor -cdx $arcbase.gz > $LOCALCDXDIR/$arcbase.cdx;
	cdxstatus=$?
	if [ $cdxstatus -ne 0 ]; then
                rm -f $LOCALCDXDIR/$arcbase.cdx;
                echo "$arcbase cdx-gen-fail $cdxstatus"
                exit 3
        fi

	gzip $LOCALCDXDIR/$arcbase.cdx;
	cdxstatus=$?
	if [ $cdxstatus -ne 0 ]; then
                rm -f $LOCALCDXDIR/$arcbase.cdx.gz;
                echo "$arcbase cdx-gz-fail $cdxstatus"
                exit 4
        fi
	echo "$arcbase success 0";
done
echo "Job complete!"
