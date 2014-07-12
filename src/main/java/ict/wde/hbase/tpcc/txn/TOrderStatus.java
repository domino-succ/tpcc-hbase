package ict.wde.hbase.tpcc.txn;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.tpcc.Const;
import ict.wde.hbase.tpcc.Utils;
import ict.wde.hbase.tpcc.table.Customer;
import ict.wde.hbase.tpcc.table.District;
import ict.wde.hbase.tpcc.table.Order;
import ict.wde.hbase.tpcc.table.OrderLine;
import ict.wde.hbase.tpcc.table.Warehouse;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class TOrderStatus extends TpccTransaction {

  private static String LINE1 = String.format("%29sOrder-Status\n", "");
  private static String LINE2 = "Warehouse: %05d  District: %02d\n";
  private static String LINE3 = "Customer: %s   Name: %-16s %2s %-16s\n";
  private static String LINE4 = "Cust-Balance: %8.2f\n";
  private static String LINE5 = "\n";
  private static String LINE6 = "Order-Number: %1$s   Entry-Date: %2$td-%2$tm-%2$tY %2$tH:%2$tM:%2$tS   Carrier-Number: %3$s\n";
  private static String LINE7 = "Supply-W     Item-Id    Qty     Amount      Delivery-Date\n";
  private static String LINEL = "  %1$s       %2$s     %3$s     $%4$ 8.2f      %5$s\n";

  private int d_id;
  private String c_last;
  private int c_id;

  public TOrderStatus(int w_id, HBaseConnection connection) {
    super(w_id, connection);
  }

  @Override
  public void generateInputData() {
    d_id = Utils.randomDid();
    int y = Utils.random(1, 100);
    if (y <= 60) {
      c_last = Utils.lastName(Utils.NURand_C_LAST());
    }
    else {
      c_last = null;
      c_id = Utils.NURand_C_ID();
    }
  }

  @Override
  public void execute(HBaseConnection conn, StringBuffer output)
      throws IOException {
    // Customer id-read
    byte[] wid = Warehouse.toRowkey(w_id());
    byte[] did = District.toDid(d_id);
    byte[] cid;
    if (this.c_last != null) {
      byte[][] cirange = Customer.toLastIndexQuery(wid, did,
          this.c_last.getBytes());
      Scan ciscan = new Scan(cirange[0], cirange[1]);
      ciscan.addFamily(Const.TEXT_FAMILY);
      ResultScanner cirs = conn.scan(ciscan, Customer.TABLE_INDEX_LAST);
      LinkedList<byte[]> ckeys = new LinkedList<byte[]>();
      for (Result r : cirs) {
        Map<byte[], byte[]> familyMap = r.getFamilyMap(Const.TEXT_FAMILY);
        for (byte[] key : familyMap.keySet()) {
          ckeys.add(familyMap.get(key));
        }
      }
      Iterator<byte[]> it = ckeys.iterator();
      for (int i = 0, j = ckeys.size() / 2; i < j; ++i) {
        it.next();
      }
      cid = Customer.toCid(it.next());
    }
    else {
      cid = Customer.toCid(c_id);
    }
    // Customer-read
    byte[] ckey = Customer.toRowkey(wid, did, cid);
    Get cget = new Get(ckey);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_FIRST);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_MIDDLE);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_LAST);
    cget.addColumn(Const.NUMERIC_FAMILY, Customer.C_BALANCE);
    Result cres = conn.get(cget, Customer.TABLE);
    String c_first = new String(cres.getValue(Const.TEXT_FAMILY,
        Customer.C_FIRST));
    String c_middle = new String(cres.getValue(Const.TEXT_FAMILY,
        Customer.C_MIDDLE));
    String c_last = new String(
        cres.getValue(Const.TEXT_FAMILY, Customer.C_LAST));
    long c_balance = Utils.b2n(cres.getValue(Const.NUMERIC_FAMILY,
        Customer.C_BALANCE));
    // Order-id-read
    byte[][] oirange = Order.toCustomerIndexQuery(wid, did, cid);
    Scan oiscan = new Scan(oirange[0], oirange[1]);
    oiscan.addColumn(Const.TEXT_FAMILY, Const.INDEX_ROW_QUALIFIER);
    ResultScanner oirs = conn.scan(oiscan, Order.TABLE_CUSTOMER_INDEX);
    Result oires = oirs.next();
    byte[] okey = oires.getValue(Const.TEXT_FAMILY, Const.INDEX_ROW_QUALIFIER);
    oirs.close();
    // Order-read
    Get oget = new Get(okey);
    oget.addColumn(Const.ID_FAMILY, Order.O_ID);
    oget.addColumn(Const.TEXT_FAMILY, Order.O_CARRIER_ID);
    oget.addColumn(Const.NUMERIC_FAMILY, Order.O_ENTRY_D);
    Result ores = conn.get(oget, Order.TABLE);
    byte[] oid = ores.getValue(Const.ID_FAMILY, Order.O_ID);
    String o_carrier_id = o_carrier_id(ores.getValue(Const.TEXT_FAMILY,
        Order.O_CARRIER_ID));
    long o_entry_d = Utils.b2n(ores.getValue(Const.NUMERIC_FAMILY,
        Order.O_ENTRY_D));

    // Output-head
    output.append(LINE1);
    output.append(String.format(LINE2, w_id(), d_id));
    output.append(String.format(LINE3, new String(cid), c_first, c_middle,
        c_last));
    output.append(String.format(LINE4, amount(c_balance)));
    output.append(LINE5);
    output.append(String
        .format(LINE6, new String(oid), o_entry_d, o_carrier_id));
    output.append(LINE7);

    // Order-Line-read
    byte[][] olrange = OrderLine.toOrderQuery(wid, did, oid);
    Scan olscan = new Scan(olrange[0], olrange[1]);
    olscan.addColumn(Const.TEXT_FAMILY, OrderLine.OL_I_ID);
    olscan.addColumn(Const.TEXT_FAMILY, OrderLine.OL_SUPPLY_W_ID);
    olscan.addColumn(Const.NUMERIC_FAMILY, OrderLine.OL_QUANTITY);
    olscan.addColumn(Const.NUMERIC_FAMILY, OrderLine.OL_AMOUNT);
    olscan.addColumn(Const.NUMERIC_FAMILY, OrderLine.OL_DELIVERY_D);
    ResultScanner olrs = conn.scan(olscan, OrderLine.TABLE);
    for (Result olres : olrs) {
      String ol_i_id = new String(olres.getValue(Const.TEXT_FAMILY,
          OrderLine.OL_I_ID));
      String ol_supply_w_id = new String(olres.getValue(Const.TEXT_FAMILY,
          OrderLine.OL_SUPPLY_W_ID));
      long ol_quantity = Utils.b2n(olres.getValue(Const.NUMERIC_FAMILY,
          OrderLine.OL_QUANTITY));
      long ol_amount = Utils.b2n(olres.getValue(Const.NUMERIC_FAMILY,
          OrderLine.OL_AMOUNT));
      String ol_delivery_d = ol_delivery_d(olres.getValue(Const.NUMERIC_FAMILY,
          OrderLine.OL_DELIVERY_D));
      output.append(String.format(LINEL, ol_supply_w_id, ol_i_id, ol_quantity,
          amount(ol_amount), ol_delivery_d));
    }
    olrs.close();
  }

  private static double amount(long amount) {
    return amount / 100.0;
  }

  private static String o_carrier_id(byte[] value) {
    if (value == null) return "";
    return new String(value);
  }

  private static String ol_delivery_d(byte[] value) {
    if (value == null || value.length == 0) return "NOT DELIVERED";
    long d = Utils.b2n(value);
    return String.format("%1$td-%1$tm-%1$tY", d);
  }

  @Override
  public boolean shouldCommit() {
    return true;
  }

  @Override
  public void waitForKeying() {
    Utils.sleep(2000);
  }

  @Override
  public void waitForThinking() {
    Utils.sleep(Utils.thinkingTime(10));
  }

  @Override
  public String getReportMessage() {
    return Character.toString(TpccTransaction.ORDER_STATUS);
  }

  @Override
  public char getType() {
    return TpccTransaction.ORDER_STATUS;
  }

}
