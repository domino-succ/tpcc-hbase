
totalw=2400
stepw=20

mstep=$(( $totalw / 10 ))
i=0

for w in `seq 0 $mstep $(( $totalw - $mstep ))`; do

from=$w
to=$(( $w + $mstep - 1 ))
echo "Starting on vm$i: $from - $to, step = $stepw"
ssh vm$i "/root/tpcc/pop-data.sh $from $to $stepw"
i=$(( i + 1 ))

done
