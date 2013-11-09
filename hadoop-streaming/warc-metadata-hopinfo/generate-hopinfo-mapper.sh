#!/usr/bin/env bash
# Author: vinay

# Mapper: Generate and Store HOPINFO files
HADOOPDIR=/home/webcrawl/hadoop-0.20.2-cdh3u3/
PROJECTDIR=`pwd`

HADOOPCMD=$HADOOPDIR/bin/hadoop
IAHADOOPTOOLS=$PROJECTDIR/lib/ia-hadoop-tools-jar-with-dependencies.jar

WARCMETADATAEXTRACTORDIR=$PROJECTDIR/warc-metadata-parser/
PYTHONPATH=$WARCMETADATAEXTRACTORDIR/warctools/
`echo export PYTHONPATH`

#replace exit statements with continue if you want Job to proceed despite some failures
while read lineoffset warcbase warcdir hopinfodir; do

	#lineoffset is ignored	
	$HADOOPCMD fs -get $warcdir/$warcbase.gz .
	copystatus=$?
	if [ $copystatus -ne 0 ]; then 
		rm -f $warcbase.gz
		echo "$warcbase warc-copy-fail $copystatus"
		exit 1
        fi

	$WARCMETADATAEXTRACTORDIR/warc_metadata_parser.py --parseType=hopinfo $warcbase.gz > $warcbase.hopinfo;
	hopinfostatus=$?
	if [ $hopinfostatus -ne 0 ]; then
                rm -f $warcbase.gz $warcbase.hopinfo;
                echo "$warcbase hopinfo-gen-fail $hopinfostatus"
                exit 2
        fi

	gzip $warcbase.hopinfo;
	hopinfostatus=$?
	if [ $hopinfostatus -ne 0 ]; then
                rm -f $warcbase.gz $warcbase.hopinfo.gz;
                echo "$warcbase hopinfo-gz-fail $hopinfostatus"
                exit 3
        fi

	$HADOOPCMD fs -put $warcbase.hopinfo.gz $hopinfodir/$warcbase.hopinfo.gz
	storestatus=$?
	if [ $storestatus -ne 0 ]; then
                rm -f $warcbase.gz $warcbase.hopinfo.gz;
		$HADOOPCMD fs -rm $hopinfodir/$warcbase.hopinfo.gz
                echo "$warcbase hopinfo-store-fail $storestatus"
                exit 4
        fi
	
	rm -f $warcbase.gz $warcbase.hopinfo.gz;
	echo "$warcbase success 0";
done
