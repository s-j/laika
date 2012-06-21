#!/bin/bash
DIR="$( cd -P "$( dirname "$0" )" && pwd )"
#setup CLASSPATH
CLASSPATH=$DIR/bin
for jar in $DIR/lib/*.jar; do
    if [ ! -n "$CLASSPATH" ]
    then
        CLASSPATH=$jar
    else
        CLASSPATH=$CLASSPATH:$jar
    fi
done

MINMEMORY=1000m
MAXMEMORY=7000m

JAVA_OPTIONS="-Dfile.encoding=UTF-8 -server -XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -Xms$MINMEMORY -XX:+DoEscapeAnalysis -Xms$MINMEMORY -Xmx$MAXMEMORY"

echo $JAVA_HOME/bin/java $JAVA_OPTIONS -cp $CLASSPATH com.ntnu.laika.distributed.dp.DPKernel $@
$JAVA_HOME/bin/java $JAVA_OPTIONS -cp $CLASSPATH com.ntnu.laika.distributed.dp.DPKernel $@
