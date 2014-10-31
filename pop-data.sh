SCRIPT_NAME=$0
BIN_DIR=`dirname ${SCRIPT_NAME}`
BASE_DIR="${BIN_DIR}"
BASE_LIB=${BASE_DIR}/lib

source $BASE_DIR/params.sh

CP=`echo $BASE_DIR/tpcc*.jar`:`echo ${BASE_LIB}/*.jar | sed 's/ /:/g'`
VM_ARGS="-DW=$W -DC_C_ID=$C_C_ID_LOAD -DC_C_LAST=$C_C_LAST_LOAD -DC_OL_I_ID=$C_OL_I_ID_LOAD"
#VM_ARGS="-DW=$W"

W_FROM=$1
W_TO=$2
STEP=$3

ZK_ADDR=processor018:2181

echo Populating Items...
nohup java $VM_ARGS -cp $CP ict.wde.hbase.tpcc.population.Main $ZK_ADDR i > $BASE_DIR/pop-item.log 2>&1 &
for w in `seq $W_FROM $STEP $W_TO`; do
echo Populating Warehouse w = $w...
# wsdco
nohup java $VM_ARGS -DPOP_W_FROM=$w -DPOP_W_TO=$(( $w + $STEP - 1 )) -cp $CP ict.wde.hbase.tpcc.population.Main $ZK_ADDR wsdco > $BASE_DIR/pop$w.log 2>&1 &
done
