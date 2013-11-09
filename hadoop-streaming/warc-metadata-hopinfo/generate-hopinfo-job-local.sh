#!/usr/bin/env bash
# Author: vinay

# Runs a Job to generate HOPINFO files for WARC files stored in a local directory.

if [ $# != 2 ] ; then
    echo "Usage: TOOL <LOCALWARCDIR> <LOCALHOPINFODIR>"
    echo "LOCALWARCDIR: LOCAL directory location containing WARC files"
    echo "LOCALHOPINFODIR: LOCAL directory location for the resulting HOPINFO files"
    exit 1
fi

LOCALWARCDIR=$1
LOCALHOPINFODIR=$2

PROJECTDIR=`pwd`
IAHADOOPTOOLS=$PROJECTDIR/lib/ia-hadoop-tools-jar-with-dependencies.jar

WARCMETADATAEXTRACTORDIR=$PROJECTDIR/warc-metadata-parser/
PYTHONPATH=$PROJECTDIR/warctools/
`echo export PYTHONPATH`

mkdir -p $LOCALHOPINFODIR
if [ $? -ne 0 ]; then
        echo "Unable to create $LOCALHOPINFODIR"
        exit 2
fi

cd $LOCALWARCDIR

#replace exit statements with continue if you want Job to proceed despite some failures
ls | grep warc.gz$ | while read warcfile; do 
	warcbase=${warcfile%%.gz}
	$WARCMETADATAEXTRACTORDIR/warc_metadata_parser.py --parseType=hopinfo $warcbase.gz > $LOCALHOPINFODIR/$warcbase.hopinfo;
	hopinfostatus=$?
	if [ $hopinfostatus -ne 0 ]; then
                rm -f $LOCALHOPINFODIR/$warcbase.hopinfo;
                echo "$warcbase hopinfo-gen-fail $hopinfostatus"
                exit 3
        fi

	gzip $LOCALHOPINFODIR/$warcbase.hopinfo;
	hopinfostatus=$?
	if [ $hopinfostatus -ne 0 ]; then
                rm -f $LOCALHOPINFODIR/$warcbase.hopinfo.gz;
                echo "$warcbase hopinfo-gz-fail $hopinfostatus"
                exit 4
        fi
	echo "$warcbase success 0";
done
echo "Job complete!"
