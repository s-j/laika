pkill -f Kernel
kernel="/home/simonj/Laika/binsh/minikernelTP.sh"
index="/mnt/data/data/laika_v2/index-dist"
param="0/10/1/3/100"
ssh simonj@traktor "$kernel 12345/- $index/0 $param" &
ssh -f simonj@traktor "$kernel 12346/localhost:12345 $index/1 $param"
ssh -f simonj@traktor "$kernel 12347/localhost:12345 $index/2 $param"
ssh -f simonj@traktor "$kernel 12348/localhost:12345 $index/3 $param"
ssh -f simonj@traktor "$kernel 12349/localhost:12345 $index/4 $param"
ssh -f simonj@traktor "$kernel 12350/localhost:12345 $index/5 $param"
ssh -f simonj@traktor "$kernel 12351/localhost:12345 $index/6 $param"
ssh -f simonj@traktor "$kernel 12352/localhost:12345 $index/7 $param"
ssh -f simonj@traktor "$kernel 12353/localhost:12345 $index/8 $param"
wait
