package ict.wde.hbase.tpcc.table;

import ict.wde.hbase.tpcc.Utils;

public class OrderLine {

  public static final byte[] TABLE = "ORDER-LINE".getBytes();

  public static final int OL_ID_LEN = 2;

  public static final byte[] MIN_OL_ID = "00".getBytes();
  public static final byte[] MAX_OL_ID = "99".getBytes();

  public static final byte[] OL_O_ID = "OL_O_ID".getBytes();
  public static final byte[] OL_D_ID = "OL_D_ID".getBytes();
  public static final byte[] OL_W_ID = "OL_W_ID".getBytes();
  public static final byte[] OL_NUMBER = "OL_NUMBER".getBytes();
  public static final byte[] OL_I_ID = "OL_I_ID".getBytes();
  public static final byte[] OL_SUPPLY_W_ID = "OL_SUPPLY_W_ID".getBytes();
  public static final byte[] OL_DELIVERY_D = "OL_DELIVERY_D".getBytes();
  public static final byte[] OL_QUANTITY = "OL_QUANTITY".getBytes();
  public static final byte[] OL_AMOUNT = "OL_AMOUNT".getBytes();
  public static final byte[] OL_DIST_INFO = "OL_DIST_INFO".getBytes();

  public static byte[] toOlid(int id) {
    return String.format("%02d", id).getBytes();
  }

  public static byte[] toRowkey(byte[] w_id, byte[] d_id, byte[] o_id,
      byte[] ol_id) {
    return Utils.concat(w_id, d_id, o_id, ol_id);
  }

  public static byte[][] toOrderQuery(byte[] w_id, byte[] d_id, byte[] o_id) {
    byte[][] ret = new byte[2][];
    ret[0] = toRowkey(w_id, d_id, o_id, "00".getBytes());
    ret[1] = toRowkey(w_id, d_id, o_id, "99".getBytes());
    return ret;
  }

}
