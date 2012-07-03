#!/bin/bash
#$ -N laika 
#$ -S /bin/bash
#$ -pe mpich 8 
#$ -V
#$ -j y
#$ -cwd
runs=($(cat ~/todo))
run=${runs[0]}
hostname="clustis3"
numnodes=8

echo Running server on $hostname
ssh simonj@$hostname "/share/apps/dropcache"
ssh simonj@$hostname "cp -r ~/Laika/lib /tmp/playground/"
ssh simonj@$hostname "cp -r ~/Laika/bin /tmp/playground/"
ssh simonj@$hostname "cp -r ~/Laika/binsh/kernelTPHPQP.sh /tmp/playground/kernel.sh"
ssh simonj@$hostname "/tmp/playground/kernel.sh 12339/- /tmp/playground/data/ $run" &

for i in $(seq 0 $(($numnodes-1)))
do
	hostname=compute-0-$i
	portnr=$(($i+12340))
	echo Starting $hostname
	ssh simonj@$hostname "/share/apps/dropcache"
	ssh simonj@$hostname "cp -r ~/Laika/lib /tmp/playground/"
	ssh simonj@$hostname "cp -r ~/Laika/bin /tmp/playground/"
	ssh simonj@$hostname "cp -r ~/Laika/binsh/kernelTPHPQP.sh /tmp/playground/kernel.sh"
	ssh -f simonj@$hostname  "/tmp/playground/kernel.sh $portnr/clustis3:12339 /tmp/playground/data/ $run"
done

wait

#delete files
for i in $(seq 0 $(($numnodes-1)))
do
	hostname=compute-0-$i
 	ssh simonj@$hostname "pkill -f Kernel"
	echo Done with $hostname
done
pkill -f Kernel

echo "INFO - conc level $run"
nruns=${#runs[@]}
rm ~/todo
touch ~/todo
if [ "$nruns" -gt "1" ]
then
	for i in $(seq 1 $nruns)
	do
		echo ${runs[$i]} >> ~/todo
	done
	ssh simonj@clustis3 "qsub ~/Laika/binsh/runTPHPQP.sh"
fi
echo Done!
