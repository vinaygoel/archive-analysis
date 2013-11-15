#!/usr/bin/env bash
# Author: vinay

# Mapper: Generate and Store WAT files
#HADOOP_HOME=/home/webcrawl/hadoop-0.20.2-cdh3u3/
PROJECTDIR=`pwd`

HADOOPCMD=$HADOOP_HOME/bin/hadoop
IAHADOOPTOOLS=$PROJECTDIR/lib/ia-hadoop-tools-jar-with-dependencies.jar

#replace exit statements with continue if you want Job to proceed despite some failures
while read lineoffset arcbase arcdir watdir; do

	#lineoffset is ignored	
	$HADOOPCMD fs -get $arcdir/$arcbase.gz .
	copystatus=$?
	if [ $copystatus -ne 0 ]; then 
		rm -f $arcbase.gz
		echo "$arcbase arc-copy-fail $copystatus"
		exit 1
        fi

	java -Xmx2048m -jar $IAHADOOPTOOLS extractor -wat $arcbase.gz > $arcbase.wat.gz;
	watstatus=$?
	if [ $watstatus -ne 0 ]; then
                rm -f $arcbase.gz $arcbase.wat.gz;
                echo "$arcbase wat-gen-fail $watstatus"
                exit 2
        fi
	
	$HADOOPCMD fs -put $arcbase.wat.gz $watdir/$arcbase.wat.gz
	storestatus=$?
	if [ $storestatus -ne 0 ]; then
                rm -f $arcbase.gz $arcbase.wat.gz;
		$HADOOPCMD fs -rm $watdir/$arcbase.wat.gz
                echo "$arcbase wat-store-fail $storestatus"
                exit 4
        fi
	
	rm -f $arcbase.gz $arcbase.wat.gz;
	echo "$arcbase success 0";
done

