package ict.wde.hbase.tpcc.table;

import ict.wde.hbase.tpcc.Utils;

public class Stock {

  public static final byte[] TABLE = "STOCK".getBytes();

  public static final byte[] S_I_ID = "S_I_ID".getBytes();
  public static final byte[] S_W_ID = "S_W_ID".getBytes();
  public static final byte[] S_QUANTITY = "S_QUANTITY".getBytes();
  public static final byte[] S_YTD = "S_YTD".getBytes();
  public static final byte[] S_ORDER_CNT = "S_ORDER_CNT".getBytes();
  public static final byte[] S_REMOTE_CNT = "S_REMOTE_CNT".getBytes();
  public static final byte[] S_DATA = "S_DATA".getBytes();
  public static final byte[][] S_DIST = new byte[10][];

  static {
    for (int i = 1; i <= 10; ++i) {
      Stock.S_DIST[i - 1] = String.format("S_DIST_%02d", i).getBytes();
    }
  }

  public static final byte[] toRowkey(int wid, int iid) {
    byte[] w_id = Warehouse.toRowkey(wid);
    byte[] i_id = Item.toRowkey(iid);
    return Stock.toRowkey(w_id, i_id);
  }

  public static final byte[] toRowkey(byte[] w_id, byte[] i_id) {
    return Utils.concat(w_id, i_id);
  }

}
