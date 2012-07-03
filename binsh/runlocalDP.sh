pkill -f Kernel
kernel="/home/simonj/Laika/binsh/minikernelDP.sh"
index="/mnt/data/data/ENVERIDX/4.DPHP.RNO"
param="0/10/1/0/100"
numnodes=4

ssh simonj@traktor "$kernel 12345/- $index/0 $param" &
for i in $(seq 1 $numnodes)
do
	ssh -f simonj@traktor "$kernel $(($i + 12345))/localhost:12345 $index/$i $param"
done
wait
