
total=160
test_run=t$total

echo "Starting test run for $total warehouses"

cd /root/tpcc; ./start-m-center.sh $test_run
sleep 3

step=$(( $total / 10 ))

w=0

for i in `seq 0 9`; do

ssh vm$i "/root/tpcc/start-terminals.sh $w $(( $w + $step - 1 )) $test_run"

w=$(( $w + $step ))

done
