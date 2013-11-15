#!/usr/bin/env bash
# Author: vinay

# Runs a Job to generate WAT files for ARC files stored in a local directory.

if [ $# != 2 ] ; then
    echo "Usage: TOOL <LOCALARCDIR> <LOCALWATDIR>"
    echo "LOCALARCDIR: LOCAL directory location containing ARC files"
    echo "LOCALWATDIR: LOCAL directory location for the resulting WAT files"
    exit 1
fi

LOCALARCDIR=$1
LOCALWATDIR=$2

PROJECTDIR=`pwd`
IAHADOOPTOOLS=$PROJECTDIR/lib/ia-hadoop-tools-jar-with-dependencies.jar

mkdir -p $LOCALWATDIR
if [ $? -ne 0 ]; then
        echo "Unable to create $LOCALWATDIR"
        exit 2
fi

cd $LOCALARCDIR

#replace exit statements with continue if you want Job to proceed despite some failures
ls | grep arc.gz$ | while read arcfile; do 
	arcbase=${arcfile%%.gz}
	java -Xmx2048m -jar $IAHADOOPTOOLS extractor -wat $arcbase.gz > $LOCALWATDIR/$arcbase.wat.gz;
	watstatus=$?
	if [ $watstatus -ne 0 ]; then
                rm -f $LOCALWATDIR/$arcbase.wat.gz;
                echo "$arcbase wat-gen-fail $watstatus"
                exit 3
        fi
	echo "$arcbase success 0";
done
echo "Job complete!"
