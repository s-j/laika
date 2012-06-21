#!/bin/bash
#find the parent dir -> LAIKAHOME
DIR="$( cd -P "$( dirname "$0" )/.." && pwd )"

#add ./bin and all ./lib/*.jar to the classpath
CLASSPATH=$DIR/bin
for jar in $DIR/lib/*.jar; do
    if [ ! -n "$CLASSPATH" ]
    then
        CLASSPATH=$jar
    else
        CLASSPATH=$CLASSPATH:$jar
    fi
done
#echo $CLASSPATH
                  
MINMEMORY=1G
MAXMEMORY=32G
java -Dfile.encoding=UTF-8 -server -XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -XX:+DoEscapeAnalysis -Xms$MINMEMORY -Xmx$MAXMEMORY -cp $CLASSPATH $1 ${@:2}
