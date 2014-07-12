package ict.wde.hbase.tpcc.table;

public class NewOrder {

  public static final byte[] TABLE = "NEW-ORDER".getBytes();

  public static final byte[] NO_O_ID = "NO_O_ID".getBytes();
  public static final byte[] NO_D_ID = "NO_D_ID".getBytes();
  public static final byte[] NO_W_ID = "NO_W_ID".getBytes();

  public static byte[] toRowkey(byte[] w_id, byte[] d_id, byte[] o_id) {
    return Order.toRowkey(w_id, d_id, o_id);
  }

}
