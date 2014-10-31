SCRIPT_NAME=$0
BIN_DIR=`dirname ${SCRIPT_NAME}`
BASE_DIR="${BIN_DIR}"
BASE_LIB=${BASE_DIR}/lib

source $BASE_DIR/params.sh

CP=`echo $BASE_DIR/tpcc*.jar`:`echo ${BASE_LIB}/*.jar | sed 's/ /:/g'`
#VM_ARGS="-DW=$W -DC_C_ID=$C_C_ID_LOAD -DC_C_LAST=$C_C_LAST_LOAD -DC_OL_I_ID=$C_OL_I_ID_LOAD"
VM_ARGS="-DW=$W"

ZK_ADDR=processor018:2181
RPC_ADDR=vm0:1988
OUTPUT=/root/tpcc/thread-output

TEST_RUN=$1
OUTPUT=$OUTPUT-$TEST_RUN
mkdir $OUTPUT

nohup java $VM_ARGS -cp $CP ict.wde.hbase.tpcc.TpccMeasurement $ZK_ADDR $OUTPUT $RPC_ADDR > $OUTPUT/m-center.out 2>&1 &
