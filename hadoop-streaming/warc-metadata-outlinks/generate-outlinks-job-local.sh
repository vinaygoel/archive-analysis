#!/usr/bin/env bash
# Author: vinay

# Runs a Job to generate OUTLINKS files for WARC files stored in a local directory.

if [ $# != 2 ] ; then
    echo "Usage: TOOL <LOCALWARCDIR> <LOCALOUTLINKSDIR>"
    echo "LOCALWARCDIR: LOCAL directory location containing WARC files"
    echo "LOCALOUTLINKSDIR: LOCAL directory location for the resulting OUTLINKS files"
    exit 1
fi

LOCALWARCDIR=$1
LOCALOUTLINKSDIR=$2

PROJECTDIR=/home/vinay/github-projects/archive-analysis/
IAHADOOPTOOLS=$PROJECTDIR/lib/ia-hadoop-tools-jar-with-dependencies.jar

WARCMETADATAEXTRACTORDIR=$PROJECTDIR/warc-metadata-parser/
PYTHONPATH=$WARCMETADATAEXTRACTORDIR/warctools/
`echo export PYTHONPATH`

mkdir -p $LOCALOUTLINKSDIR
if [ $? -ne 0 ]; then
        echo "Unable to create $LOCALOUTLINKSDIR"
        exit 2
fi

cd $LOCALWARCDIR

#replace exit statements with continue if you want Job to proceed despite some failures
ls | grep warc.gz$ | while read warcfile; do 
	warcbase=${warcfile%%.gz}
	$WARCMETADATAEXTRACTORDIR/warc_metadata_parser.py --parseType=outlinks $warcbase.gz > $LOCALOUTLINKSDIR/$warcbase.outlinks;
	outlinksstatus=$?
	if [ $outlinksstatus -ne 0 ]; then
                rm -f $LOCALOUTLINKSDIR/$warcbase.outlinks;
                echo "$warcbase outlinks-gen-fail $outlinksstatus"
                exit 3
        fi

	gzip $LOCALOUTLINKSDIR/$warcbase.outlinks;
	outlinksstatus=$?
	if [ $outlinksstatus -ne 0 ]; then
                rm -f $LOCALOUTLINKSDIR/$warcbase.outlinks.gz;
                echo "$warcbase outlinks-gz-fail $outlinksstatus"
                exit 4
        fi
	echo "$warcbase success 0";
done
echo "Job complete!"
