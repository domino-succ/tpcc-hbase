SCRIPT_NAME=$0
BIN_DIR=`dirname ${SCRIPT_NAME}`
BASE_DIR="${BIN_DIR}"
BASE_LIB=${BASE_DIR}/lib

CP=`echo tpcc*.jar`:`echo ${BASE_LIB}/*.jar | sed 's/ /:/g'`

TABLE_CONF=$BASE_DIR/table.conf
ZK_ADDR=processor018:2181
DROP_OLD_TABLES=true

java -cp $CP ict.wde.hbase.tpcc.population.TableCreator $TABLE_CONF $ZK_ADDR $DROP_OLD_TABLES
