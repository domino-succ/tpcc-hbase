package ict.wde.hbase.tpcc.table;

import ict.wde.hbase.tpcc.Utils;

import java.util.Arrays;

public class Customer {

  public static final byte[] TABLE = "CUSTOMER".getBytes();
  public static final byte[] TABLE_INDEX_LAST = "CUSTOMER_INDEX_LAST"
      .getBytes();

  public static final int C_ID_LEN = 5;

  public static final byte[] C_ID = "C_ID".getBytes();
  public static final byte[] C_D_ID = "C_D_ID".getBytes();
  public static final byte[] C_W_ID = "C_W_ID ".getBytes();
  public static final byte[] C_LAST = "C_LAST".getBytes();
  public static final byte[] C_MIDDLE = "C_MIDDLE".getBytes();
  public static final byte[] C_FIRST = "C_FIRST".getBytes();
  public static final byte[] C_STREET_1 = "C_STREET_1".getBytes();
  public static final byte[] C_STREET_2 = "C_STREET_2".getBytes();
  public static final byte[] C_CITY = "C_CITY".getBytes();
  public static final byte[] C_STATE = "C_STATE".getBytes();
  public static final byte[] C_ZIP = "C_ZIP".getBytes();
  public static final byte[] C_PHONE = "C_PHONE".getBytes();
  public static final byte[] C_SINCE = "C_SINCE".getBytes();
  public static final byte[] C_CREDIT = "C_CREDIT".getBytes();
  public static final byte[] C_CREDIT_LIM = "C_CREDIT_LIM".getBytes();
  public static final byte[] C_DISCOUNT = "C_DISCOUNT".getBytes();
  public static final byte[] C_BALANCE = "C_BALANCE".getBytes();
  public static final byte[] C_YTD_PAYMENT = "C_YTD_PAYMENT".getBytes();
  public static final byte[] C_PAYMENT_CNT = "C_PAYMENT_CNT".getBytes();
  public static final byte[] C_DELIVERY_CNT = "C_DELIVERY_CNT".getBytes();
  public static final byte[] C_DATA = "C_DATA".getBytes();

  public static byte[] toCid(int cid) {
    return String.format("%05d", cid).getBytes();
  }

  public static byte[] toRowkey(int wid, int did, int cid) {
    return toRowkey(Warehouse.toRowkey(wid), District.toDid(did), toCid(cid));
  }

  public static byte[] toRowkey(byte[] wid, byte[] did, byte[] cid) {
    return Utils.concat(wid, did, cid);
  }

  public static byte[] toCid(byte[] rowkey) {
    return Arrays.copyOfRange(rowkey, Warehouse.W_ID_LEN + District.D_ID_LEN,
        rowkey.length);
  }

  private static final byte[] INDEX_DELIMITER = { 0 };

  public static byte[] toLastIndexRowkey(byte[] w_id, byte[] d_id, byte[] last,
      byte[] first) {
    return Utils.concat(w_id, d_id, last, INDEX_DELIMITER, first);
  }

  public static byte[][] toLastIndexQuery(byte[] w_id, byte[] d_id, byte[] last) {
    byte[][] range = new byte[2][];
    range[0] = Utils.concat(w_id, d_id, last, INDEX_DELIMITER);
    range[1] = Arrays.copyOf(range[0], range[0].length);
    ++range[1][range[1].length - 1]; // To make it the upper bound
    return range;
  }

}
