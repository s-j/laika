#!/bin/bash
#$ -N copydog 
#$ -S /bin/bash
#$ -pe mpich 8 
#$ -V
#$ -j y
#$ -cwd
index=/export/work/simonj/index-dp

#copy files
for i in $(seq 0 7)
do
	hostname=compute-0-$i
	echo Storing files on $hostname
	ssh $hostname "mkdir /tmp/playground"
	ssh $hostname "mkdir /tmp/playground/data"
	scp $index/$(($i+1))/* $hostname:/tmp/playground/data
done
hostname="clustis3"
ssh $hostname "mkdir /tmp/playground/"
ssh $hostname "mkdir /tmp/playground/data"
scp $index/0/* $hostname:/tmp/playground/data
