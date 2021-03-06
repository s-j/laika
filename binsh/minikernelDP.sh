#!/bin/bash
DIR="$( cd -P "$( dirname "$0" )"/.. && pwd )"
#setup CLASSPATH
CLASSPATH=$DIR/bin:$CLASSPATH
for jar in $DIR/lib/*.jar; do
	if [ ! -n "$CLASSPATH" ]
	then
		CLASSPATH=$jar
	else
		CLASSPATH=$CLASSPATH:$jar
	fi
done

MINMEMORY=100m
MAXMEMORY=800m
JAVA_OPTIONS="-Dfile.encoding=UTF-8 -server -XX:+UseParallelGC -XX:+AggressiveOpts\
 -XX:+UseFastAccessorMethods -XX:+DoEscapeAnalysis -Xms$MINMEMORY -Xmx$MAXMEMORY"

echo java  $JAVA_OPTIONS -cp $CLASSPATH com.ntnu.laika.distributed.dp.DPKernel $@
java $JAVA_OPTIONS -cp $CLASSPATH com.ntnu.laika.distributed.dp.DPKernel $@
