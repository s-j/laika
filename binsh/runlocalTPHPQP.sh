pkill -f Kernel
kernel="/home/simonj/Laika/binsh/minikernelTPHPQP.sh"
index="/mnt/data/data/ENVERIDX/4.ZHANG.RNO"
param="0/50/1/glb/or"
numnodes=4

ssh simonj@traktor "$kernel 12345/- $index/0 $param" &
for i in $(seq 1 $numnodes)
do
	ssh -f simonj@traktor "$kernel $(($i + 12345))/localhost:12345 $index/$i $param"
done
wait
