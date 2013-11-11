#!/usr/bin/env bash
# Author: vinay

# Mapper: Generate and Store WAT files
#HADOOP_HOME=/home/webcrawl/hadoop-0.20.2-cdh3u3/
PROJECTDIR=`pwd`

HADOOPCMD=$HADOOP_HOME/bin/hadoop
IAHADOOPTOOLS=$PROJECTDIR/lib/ia-hadoop-tools-jar-with-dependencies.jar

#replace exit statements with continue if you want Job to proceed despite some failures
while read lineoffset warcbase warcdir watdir; do

	#lineoffset is ignored	
	$HADOOPCMD fs -get $warcdir/$warcbase.gz .
	copystatus=$?
	if [ $copystatus -ne 0 ]; then 
		rm -f $warcbase.gz
		echo "$warcbase warc-copy-fail $copystatus"
		exit 1
        fi

	java -Xmx2048m -jar $IAHADOOPTOOLS extractor -wat $warcbase.gz > $warcbase.wat.gz;
	watstatus=$?
	if [ $watstatus -ne 0 ]; then
                rm -f $warcbase.gz $warcbase.wat.gz;
                echo "$warcbase wat-gen-fail $watstatus"
                exit 2
        fi
	
	$HADOOPCMD fs -put $warcbase.wat.gz $watdir/$warcbase.wat.gz
	storestatus=$?
	if [ $storestatus -ne 0 ]; then
                rm -f $warcbase.gz $warcbase.wat.gz;
		$HADOOPCMD fs -rm $watdir/$warcbase.wat.gz
                echo "$warcbase wat-store-fail $storestatus"
                exit 4
        fi
	
	rm -f $warcbase.gz $warcbase.wat.gz;
	echo "$warcbase success 0";
done

