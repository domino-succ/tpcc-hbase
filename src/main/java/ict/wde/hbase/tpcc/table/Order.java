package ict.wde.hbase.tpcc.table;

import ict.wde.hbase.tpcc.Utils;

import org.apache.hadoop.hbase.util.Bytes;

public class Order {

  public static final byte[] TABLE = "ORDER".getBytes();
  public static final byte[] TABLE_CUSTOMER_INDEX = "ORDER_CUSTOMER_INDEX"
      .getBytes();
  public static final int O_ID_LEN = 7;
  public static final int OID_UPPER_BOUND = 9999999;
  public static final byte[] OID_UPPER_BOUND_B = toOid(OID_UPPER_BOUND);
  public static final byte[] OID_LOWER_BOUND_B = toOid(0);

  public static final byte[] O_ID = "O_ID".getBytes();
  public static final byte[] O_D_ID = "O_D_ID".getBytes();
  public static final byte[] O_W_ID = "O_W_ID".getBytes();
  public static final byte[] O_C_ID = "O_C_ID".getBytes();
  public static final byte[] O_ENTRY_D = "O_ENTRY_D".getBytes();
  public static final byte[] O_CARRIER_ID = "O_CARRIER_ID".getBytes();
  public static final byte[] O_OL_CNT = "O_OL_CNT".getBytes();
  public static final byte[] O_ALL_LOCAL = "O_ALL_LOCAL".getBytes();

  public static byte[] toOid(int id) {
    return String.format("%07d", id).getBytes();
  }

  public static byte[] toRowkey(byte[] w_id, byte[] d_id, byte[] o_id) {
    return Utils.concat(w_id, d_id, o_id);
  }

  public static byte[] toCustomerIndexRowkey(byte[] w_id, byte[] d_id,
      byte[] c_id, int o_id) {
    byte[] ret = new byte[w_id.length + d_id.length + c_id.length
        + Bytes.SIZEOF_INT];
    int offset = 0;
    Bytes.putBytes(ret, offset, w_id, 0, w_id.length);
    offset += w_id.length;
    Bytes.putBytes(ret, offset, d_id, 0, d_id.length);
    offset += d_id.length;
    Bytes.putBytes(ret, offset, c_id, 0, c_id.length);
    offset += c_id.length;
    Bytes.putInt(ret, offset, OID_UPPER_BOUND - o_id);
    return ret;
  }

  public static byte[][] toCustomerIndexQuery(byte[] w_id, byte[] d_id,
      byte[] c_id) {
    byte[][] range = new byte[2][];
    int len = w_id.length + d_id.length + c_id.length + Bytes.SIZEOF_INT;
    range[0] = new byte[len];
    range[1] = new byte[len];
    int offset = 0;
    Bytes.putBytes(range[0], offset, w_id, 0, w_id.length);
    Bytes.putBytes(range[1], offset, w_id, 0, w_id.length);
    offset += w_id.length;
    Bytes.putBytes(range[0], offset, d_id, 0, d_id.length);
    Bytes.putBytes(range[1], offset, d_id, 0, d_id.length);
    offset += d_id.length;
    Bytes.putBytes(range[0], offset, c_id, 0, c_id.length);
    Bytes.putBytes(range[1], offset, c_id, 0, c_id.length);
    offset += c_id.length;
    Bytes.putInt(range[0], offset, 0);
    Bytes.putInt(range[1], offset, OID_UPPER_BOUND);
    return range;
  }

}
