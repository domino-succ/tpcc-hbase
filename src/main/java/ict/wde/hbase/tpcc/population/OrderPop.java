package ict.wde.hbase.tpcc.population;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.tpcc.Const;
import ict.wde.hbase.tpcc.Utils;
import ict.wde.hbase.tpcc.domino.DominoDriver;
import ict.wde.hbase.tpcc.table.Customer;
import ict.wde.hbase.tpcc.table.District;
import ict.wde.hbase.tpcc.table.Item;
import ict.wde.hbase.tpcc.table.NewOrder;
import ict.wde.hbase.tpcc.table.Order;
import ict.wde.hbase.tpcc.table.OrderLine;
import ict.wde.hbase.tpcc.table.Warehouse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class OrderPop extends DataPopulation {

  static final int POP_TOTAL_ID = 3000;
  static final int NEW_ORDER_CNT = 2100;

  private int wid = POP_W_FROM;
  private int did = 0;
  private int id = 0;
  private List<byte[]> cids = new ArrayList<byte[]>(3000);

  public OrderPop(Configuration connection) {
    super(connection);
    for (int i = 0; i < 3000; ++i) {
      cids.add(Customer.toCid(i));
    }
    Collections.shuffle(cids);
    System.out.println("Poping order data from w: " + POP_W_FROM + "-"
        + POP_W_TO);
  }

  @Override
  public int popOneRow() throws IOException {
    if (wid > POP_W_TO && did >= DistrictPop.POP_TOTAL_ID && id >= POP_TOTAL_ID) {
      return 0;
    }
    byte[] w_id = Warehouse.toRowkey(wid);
    byte[] d_id = District.toDid(did);
    byte[] o_id = Order.toOid(id);
    byte[] c_id = cids.get(id);
    byte[] row = Order.toRowkey(w_id, d_id, o_id);
    byte[] time = Utils.currentTimestamp();
    int ol_cnt = Utils.random(5, 15);
    int ret = ol_cnt + 2;

    writeOrderTable(w_id, d_id, o_id, c_id, row, time, Bytes.toBytes(ol_cnt));
    writeOrderCustomerIndex(w_id, d_id, c_id, row);
    writeOrderLineTable(w_id, d_id, o_id, ol_cnt, time);
    if (id >= NEW_ORDER_CNT) {
      writeNewOrderTable(w_id, d_id, o_id);
      ++ret;
    }

    ++id;
    if (id >= POP_TOTAL_ID) {
      ++did;
      if (did >= DistrictPop.POP_TOTAL_ID) {
        if (wid > POP_W_TO) return 0;
        ++wid;
        did = 0;
      }
      Collections.shuffle(cids);
      id = 0;
    }
    return ret;
  }

  private void writeNewOrderTable(byte[] w_id, byte[] d_id, byte[] o_id)
      throws IOException {
    byte[] row = Order.toRowkey(w_id, d_id, o_id);
    Put put = new Put(row);
    put.add(Const.ID_FAMILY, NewOrder.NO_O_ID, o_id);
    put.add(Const.ID_FAMILY, NewOrder.NO_D_ID, d_id);
    put.add(Const.ID_FAMILY, NewOrder.NO_W_ID, w_id);
    put(put, NewOrder.TABLE);
  }

  private void writeOrderLineTable(byte[] w_id, byte[] d_id, byte[] o_id,
      int ol_cnt, byte[] time) throws IOException {
    for (int ol_id = 0; ol_id < ol_cnt; ++ol_id) {
      byte[] ol_number = OrderLine.toOlid(ol_id);
      byte[] row = OrderLine.toRowkey(w_id, d_id, o_id, ol_number);
      Put put = new Put(row);
      put.add(Const.ID_FAMILY, OrderLine.OL_O_ID, o_id);
      put.add(Const.ID_FAMILY, OrderLine.OL_D_ID, d_id);
      put.add(Const.ID_FAMILY, OrderLine.OL_W_ID, w_id);
      put.add(Const.ID_FAMILY, OrderLine.OL_NUMBER, ol_number);
      put.add(Const.TEXT_FAMILY, OrderLine.OL_I_ID, oliid());
      put.add(Const.TEXT_FAMILY, OrderLine.OL_SUPPLY_W_ID, w_id);
      put.add(Const.TEXT_FAMILY, OrderLine.OL_DELIVERY_D, deliveryd(time));
      put.add(Const.NUMERIC_FAMILY, OrderLine.OL_QUANTITY, QUANTITY0);
      put.add(Const.NUMERIC_FAMILY, OrderLine.OL_AMOUNT, amount());
      put.add(Const.TEXT_FAMILY, OrderLine.OL_DIST_INFO, distinfo());
      put(put, OrderLine.TABLE);
    }
  }

  private static byte[] distinfo() {
    return Utils.randomLetterString(24, 24).getBytes();
  }

  private static final byte[] AMOUNT0 = Bytes.toBytes(0L);

  private byte[] amount() {
    if (id < NEW_ORDER_CNT) return AMOUNT0;
    return Bytes.toBytes((long) Utils.random(1, 999999));
  }

  private static final byte[] QUANTITY0 = Bytes.toBytes(5L);

  private byte[] deliveryd(byte[] time) {
    if (id < NEW_ORDER_CNT) return time;
    return null;
  }

  private static byte[] oliid() {
    return Item.toRowkey(Utils.random(1, 100000));
  }

  private void writeOrderCustomerIndex(byte[] w_id, byte[] d_id, byte[] c_id,
      byte[] row) throws IOException {
    byte[] indexRow = Order.toCustomerIndexRowkey(w_id, d_id, c_id, id);
    Put put = new Put(indexRow);
    put.add(Const.TEXT_FAMILY, Const.INDEX_ROW_QUALIFIER, row);
    put(put, Order.TABLE_CUSTOMER_INDEX);
  }

  private void writeOrderTable(byte[] w_id, byte[] d_id, byte[] o_id,
      byte[] c_id, byte[] row, byte[] time, byte[] ol_cnt) throws IOException {
    Put put = new Put(row);
    put.add(Const.ID_FAMILY, Order.O_ID, o_id);
    put.add(Const.ID_FAMILY, Order.O_D_ID, d_id);
    put.add(Const.ID_FAMILY, Order.O_W_ID, w_id);
    put.add(Const.TEXT_FAMILY, Order.O_C_ID, c_id);
    put.add(Const.NUMERIC_FAMILY, Order.O_ENTRY_D, time);
    put.add(Const.TEXT_FAMILY, Order.O_CARRIER_ID, carrierid());
    put.add(Const.NUMERIC_FAMILY, Order.O_OL_CNT, ol_cnt);
    put.add(Const.NUMERIC_FAMILY, Order.O_ALL_LOCAL, ALLLOCAL0);
    put(put, Order.TABLE);
  }

  private static final byte[] ALLLOCAL0 = Bytes.toBytes(1L);

  private byte[] carrierid() {
    if (id >= NEW_ORDER_CNT) return null;
    return String.format("%02d", Utils.random(1, 10)).getBytes();
  }

  public static void main(String[] args) throws IOException {
    HBaseConnection conn = new DominoDriver()
        .getConnection("processor018:2181");
    Get get = new Get(OrderLine.toRowkey(Warehouse.toRowkey(1),
        District.toDid(1), Order.toOid(1), OrderLine.toOlid(1)));
    Result r = conn.get(get, OrderLine.TABLE);
    System.out.println(new String(r.getValue(Const.TEXT_FAMILY,
        OrderLine.OL_I_ID)));
    System.out.println(new String(r.getValue(Const.TEXT_FAMILY,
        OrderLine.OL_DIST_INFO)));
    conn.commit();
    conn.close();
  }
}
