SCRIPT_NAME=$0
BIN_DIR=`dirname ${SCRIPT_NAME}`
BASE_DIR="${BIN_DIR}"
BASE_LIB=${BASE_DIR}/lib

source $BASE_DIR/params.sh

CP=`echo $BASE_DIR/tpcc*.jar`:`echo ${BASE_LIB}/*.jar | sed 's/ /:/g'`
VM_ARGS="-DXmx256m -DW=$W -DC_C_ID=$C_C_ID_RUN -DC_C_LAST=$C_C_LAST_RUN -DC_OL_I_ID=$C_OL_I_ID_RUN"
#VM_ARGS="-DW=$W"

ZK_ADDR=processor018:2181
RPC_ADDR=vm0:1988
OUTPUT=/root/tpcc/thread-output

FROM_W=$1
TO_W=$2
TEST_TURN=$3
OUTPUT=$OUTPUT-$TEST_TURN
mkdir $OUTPUT

TOTAL=$(( $TO_W - $FROM_W + 1 ))
STEP=$(( $TOTAL / 4 ))
if [ "$STEP" = "0" ]; then
STEP=1
fi

echo "Starting terminals from Warehouse[$FROM_W] to [$TO_W]"

for w in `seq $FROM_W $STEP $(( $TO_W - $STEP + 1 ))`; do

#for d in `seq 0 9`; do
nohup java $VM_ARGS -cp $CP ict.wde.hbase.tpcc.Terminal $w-$(( $w + $STEP - 1 )) $ZK_ADDR $OUTPUT $RPC_ADDR > $OUTPUT/$w.out 2>&1 &
#done

done
