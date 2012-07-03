#!/bin/bash
#delete files
for i in $(seq 0 7)
do
 	ssh simonj@compute-0-$i "pkill -f Kernel"
	echo Killed all on compute-0-$i
done
pkill -f Kernel
echo Done!
