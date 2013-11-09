#!/usr/bin/env bash
# Author: vinay

# Mapper: Generate and Store OUTLINKS files
HADOOPDIR=/home/webcrawl/hadoop-0.20.2-cdh3u3/
PROJECTDIR=/home/vinay/github-projects/archive-analysis/

HADOOPCMD=$HADOOPDIR/bin/hadoop
IAHADOOPTOOLS=$PROJECTDIR/lib/ia-hadoop-tools-jar-with-dependencies.jar

WARCMETADATAEXTRACTORDIR=$PROJECTDIR/warc-metadata-parser/
PYTHONPATH=$WARCMETADATAEXTRACTORDIR/warctools/
`echo export PYTHONPATH`

#replace exit statements with continue if you want Job to proceed despite some failures
while read lineoffset warcbase warcdir outlinksdir; do

	#lineoffset is ignored	
	$HADOOPCMD fs -get $warcdir/$warcbase.gz .
	copystatus=$?
	if [ $copystatus -ne 0 ]; then 
		rm -f $warcbase.gz
		echo "$warcbase warc-copy-fail $copystatus"
		exit 1
        fi

	$WARCMETADATAEXTRACTORDIR/warc_metadata_parser.py --parseType=outlinks $warcbase.gz > $warcbase.outlinks;
	outlinksstatus=$?
	if [ $outlinksstatus -ne 0 ]; then
                rm -f $warcbase.gz $warcbase.outlinks;
                echo "$warcbase outlinks-gen-fail $outlinksstatus"
                exit 2
        fi

	gzip $warcbase.outlinks;
	outlinksstatus=$?
	if [ $outlinksstatus -ne 0 ]; then
                rm -f $warcbase.gz $warcbase.outlinks.gz;
                echo "$warcbase outlinks-gz-fail $outlinksstatus"
                exit 3
        fi

	$HADOOPCMD fs -put $warcbase.outlinks.gz $outlinksdir/$warcbase.outlinks.gz
	storestatus=$?
	if [ $storestatus -ne 0 ]; then
                rm -f $warcbase.gz $warcbase.outlinks.gz;
		$HADOOPCMD fs -rm $outlinksdir/$warcbase.outlinks.gz
                echo "$warcbase outlinks-store-fail $storestatus"
                exit 4
        fi
	
	rm -f $warcbase.gz $warcbase.outlinks.gz;
	echo "$warcbase success 0";
done
