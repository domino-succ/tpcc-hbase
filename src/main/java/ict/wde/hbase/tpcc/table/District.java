package ict.wde.hbase.tpcc.table;

import ict.wde.hbase.tpcc.Utils;

public class District {

  public static final byte[] TABLE = "DISTRICT".getBytes();
  public static final int D_ID_LEN = 2;

  public static final byte[] D_ID = "D_ID".getBytes();
  public static final byte[] D_W_ID = "D_W_ID".getBytes();
  public static final byte[] D_NAME = "D_NAME".getBytes();
  public static final byte[] D_STREET_1 = "D_STREET_1".getBytes();
  public static final byte[] D_STREET_2 = "D_STREET_2".getBytes();
  public static final byte[] D_CITY = "D_CITY".getBytes();
  public static final byte[] D_STATE = "D_STATE".getBytes();
  public static final byte[] D_ZIP = "D_ZIP".getBytes();
  public static final byte[] D_TAX = "D_TAX".getBytes();
  public static final byte[] D_YTD = "D_YTD".getBytes();
  public static final byte[] D_NEXT_O_ID = "D_NEXT_O_ID".getBytes();

  public static final byte[] toRowkey(int wid, int did) {
    byte[] w_id = Warehouse.toRowkey(wid);
    byte[] d_id = toDid(did);
    return toRowkey(w_id, d_id);
  }

  public static final byte[] toRowkey(byte[] w_id, byte[] d_id) {
    return Utils.concat(w_id, d_id);
  }

  public static final byte[] toDid(int did) {
    return String.format("%02d", did).getBytes();
  }

}
