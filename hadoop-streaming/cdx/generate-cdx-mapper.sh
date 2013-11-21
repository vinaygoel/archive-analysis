#!/usr/bin/env bash
# Author: vinay

# Mapper: Generate and Store CDX files
PROJECTDIR=`pwd`
HADOOPCMD=$HADOOP_HOME/bin/hadoop
IAHADOOPTOOLS=./ia-hadoop-tools-jar-with-dependencies.jar

#replace exit statements with continue if you want Job to proceed despite some failures
while read lineoffset warcbase warcdir cdxdir; do

	#lineoffset is ignored	
	$HADOOPCMD fs -get $warcdir/$warcbase.gz .
	copystatus=$?
	if [ $copystatus -ne 0 ]; then 
		rm -f $warcbase.gz
		echo "$warcbase warc-copy-fail $copystatus"
		exit 1
        fi

	java -Xmx2048m -jar $IAHADOOPTOOLS extractor -cdx $warcbase.gz > $warcbase.cdx;
	cdxstatus=$?
	if [ $cdxstatus -ne 0 ]; then
                rm -f $warcbase.gz $warcbase.cdx;
                echo "$warcbase cdx-gen-fail $cdxstatus"
                exit 2
        fi

	gzip $warcbase.cdx;
	cdxstatus=$?
	if [ $cdxstatus -ne 0 ]; then
                rm -f $warcbase.gz $warcbase.cdx.gz;
                echo "$warcbase cdx-gz-fail $cdxstatus"
                exit 3
        fi

	$HADOOPCMD fs -put $warcbase.cdx.gz $cdxdir/$warcbase.cdx.gz
	storestatus=$?
	if [ $storestatus -ne 0 ]; then
                rm -f $warcbase.gz $warcbase.cdx.gz;
		$HADOOPCMD fs -rm $cdxdir/$warcbase.cdx.gz
                echo "$warcbase cdx-store-fail $storestatus"
                exit 4
        fi
	
	rm -f $warcbase.gz $warcbase.cdx.gz;
	echo "$warcbase success 0";
done
