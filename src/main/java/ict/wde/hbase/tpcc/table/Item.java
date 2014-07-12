package ict.wde.hbase.tpcc.table;

public class Item {

  public static final byte[] TABLE = "ITEM".getBytes();
  public static final int I_ID_LEN = 5;

  public static final byte[] I_ID = "I_ID".getBytes();
  public static final byte[] I_IM_ID = "I_ID".getBytes();
  public static final byte[] I_NAME = "I_IM_ID".getBytes();
  public static final byte[] I_PRICE = "I_PRICE".getBytes();
  public static final byte[] I_DATA = "I_DATA".getBytes();

  public static byte[] toRowkey(int id) {
    return String.format("%05d", id).getBytes();
  }

}
