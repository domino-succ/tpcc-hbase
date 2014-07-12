package ict.wde.hbase.tpcc.table;

import ict.wde.hbase.tpcc.Utils;

public class History {

  public static final byte[] H_C_ID = "H_C_ID".getBytes();
  public static final byte[] H_C_D_ID = "H_C_D_ID".getBytes();
  public static final byte[] H_C_W_ID = "H_C_W_ID".getBytes();
  public static final byte[] H_D_ID = "H_D_ID".getBytes();
  public static final byte[] H_W_ID = "H_W_ID".getBytes();
  public static final byte[] H_DATE = "H_DATE".getBytes();
  public static final byte[] H_AMOUNT = "H_AMOUNT".getBytes();
  public static final byte[] H_DATA = "H_DATA".getBytes();
  public static final byte[] TABLE = "HISTORY".getBytes();

  public static byte[] toRowkey(byte[] w_id, byte[] d_id, byte[] c_id,
      byte[] time) {
    return Utils.concat(w_id, d_id, c_id, time);
  }

}
