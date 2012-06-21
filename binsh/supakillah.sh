#!/bin/bash

#delete files
for i in $(seq 0 7)
do
	ssh simonj@compute-0-$i -f "rm -rf /tmp/playground"
 	ssh simonj@compute-0-$i -f "pkill -f Kernel"
	echo Killed all on compute-0-$i
done
ssh simonj@clustis3 -f "rm -rf /tmp/playground"
ssh simonj@clustis3 -f "pkill -f Kernel"
echo Killed all on clustis3
sleep 1
echo Done!
