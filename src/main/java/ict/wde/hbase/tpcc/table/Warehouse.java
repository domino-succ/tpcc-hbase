package ict.wde.hbase.tpcc.table;

public class Warehouse {

  public static final byte[] TABLE = "WAREHOUSE".getBytes();
  public static final int W_ID_LEN = 5;

  public static byte[] W_ID = "W_ID".getBytes();
  public static byte[] W_NAME = "W_NAME".getBytes();
  public static byte[] W_STREET_1 = "W_STREET_1".getBytes();
  public static byte[] W_STREET_2 = "W_STREET_2".getBytes();
  public static byte[] W_CITY = "W_CITY".getBytes();
  public static byte[] W_STATE = "W_STATE".getBytes();
  public static byte[] W_ZIP = "W_ZIP".getBytes();
  public static byte[] W_TAX = "W_TAX".getBytes();
  public static byte[] W_YTD = "W_YTD".getBytes();

  public static byte[] toRowkey(int w_id) {
    return String.format("%05d", w_id).getBytes();
  }

}
