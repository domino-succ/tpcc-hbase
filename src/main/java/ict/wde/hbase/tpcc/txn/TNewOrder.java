package ict.wde.hbase.tpcc.txn;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.tpcc.Const;
import ict.wde.hbase.tpcc.Utils;
import ict.wde.hbase.tpcc.table.Customer;
import ict.wde.hbase.tpcc.table.District;
import ict.wde.hbase.tpcc.table.Item;
import ict.wde.hbase.tpcc.table.NewOrder;
import ict.wde.hbase.tpcc.table.Order;
import ict.wde.hbase.tpcc.table.OrderLine;
import ict.wde.hbase.tpcc.table.Stock;
import ict.wde.hbase.tpcc.table.Warehouse;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public class TNewOrder extends TpccTransaction {

  public static final String TITLE = "New Order";
  static final String LINE1 = "%39s\n";
  static final String LINE2 = "Warehouse: %05d  District: %02d                        Date: %s\n";
  static final String LINE3 = "Customer:  %05d  Name: %-16s   Credit: %2s   %%Disc: % 2.2f\n";
  static final String LINE4 = "Order Number: %07d   Number of Lines: % 2d        W_tax: %4.4f D_tax: %4.4f\n";
  static final String LINE5 = "\n";
  static final String LINE6 = "Supp_W  Item_Id  Item Name                 Qty  Stock  B/G  Price    Amount\n";
  static final String OL_LINE = " %05d  %05d    %-24s  % 2d    % 3d    %s   $% 6.2f  $%7.2f\n";
  static final String SUM_LINE = String.format("%-61sTotal:  $%%8.2f\n",
      "Execution Status: Complete");
  static final String RBK_LINE = "Execution Status: Item number is not valid\n";

  private int d_id;
  private int c_id;
  private int ol_cnt;
  private int rbk;
  private int[] i_id;
  private int[] supp_w_id;
  private int[] quantity;
  private long entry_d;

  private int all_local = 1;

  public TNewOrder(int w_id, HBaseConnection connection) {
    super(w_id, connection);
  }

  @Override
  public void execute(HBaseConnection conn, StringBuffer output)
      throws IOException {
    byte[] wid = Warehouse.toRowkey(w_id());
    byte[] did = District.toDid(d_id);
    byte[] cid = Customer.toCid(c_id);

    // Warehouse
    Get wget = new Get(wid);
    wget.addColumn(Const.NUMERIC_FAMILY, Warehouse.W_TAX);
    long w_tax = Utils.b2n(conn.get(wget, Warehouse.TABLE).getValue(
        Const.NUMERIC_FAMILY, Warehouse.W_TAX));

    // District-read
    byte[] dkey = District.toRowkey(wid, did);
    Get dget = new Get(dkey);
    dget.addColumn(Const.NUMERIC_FAMILY, District.D_TAX);
    dget.addColumn(Const.NUMERIC_FAMILY, District.D_NEXT_O_ID);
    Result dres = conn.get(dget, District.TABLE);
    long d_tax = Utils.b2n(dres.getValue(Const.NUMERIC_FAMILY, District.D_TAX));
    int d_next_oid = (int) Utils.b2n(dres.getValue(Const.NUMERIC_FAMILY,
        District.D_NEXT_O_ID));
    // District-write
    Put dput = new Put(dkey);
    dput.add(Const.NUMERIC_FAMILY, District.D_NEXT_O_ID,
        Utils.n2b(d_next_oid + 1));
    conn.put(dput, District.TABLE);
    byte[] oid = Order.toOid(d_next_oid);

    // Customer
    byte[] ckey = Customer.toRowkey(wid, did, cid);
    Get cget = new Get(ckey);
    cget.addColumn(Const.NUMERIC_FAMILY, Customer.C_DISCOUNT);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_LAST);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_CREDIT);
    Result cres = conn.get(cget, Customer.TABLE);
    long c_discount = Utils.b2n(cres.getValue(Const.NUMERIC_FAMILY,
        Customer.C_DISCOUNT));
    String c_last = new String(
        cres.getValue(Const.TEXT_FAMILY, Customer.C_LAST));
    String c_credit = new String(cres.getValue(Const.TEXT_FAMILY,
        Customer.C_CREDIT));

    // Order
    byte[] okey = Order.toRowkey(wid, did, oid);
    Put oput = new Put(okey);
    oput.add(Const.ID_FAMILY, Order.O_W_ID, wid);
    oput.add(Const.ID_FAMILY, Order.O_D_ID, did);
    oput.add(Const.ID_FAMILY, Order.O_ID, oid);
    oput.add(Const.TEXT_FAMILY, Order.O_CARRIER_ID, null);
    oput.add(Const.NUMERIC_FAMILY, Order.O_ALL_LOCAL, Utils.n2b(all_local));
    oput.add(Const.NUMERIC_FAMILY, Order.O_OL_CNT, Utils.n2b(ol_cnt));
    oput.add(Const.NUMERIC_FAMILY, Order.O_ENTRY_D, Utils.n2b(entry_d));
    conn.put(oput, Order.TABLE);
    // Order Customer Index
    byte[] oindexkey = Order.toCustomerIndexRowkey(wid, did, cid, d_next_oid);
    Put ociput = new Put(oindexkey);
    ociput.add(Const.TEXT_FAMILY, Const.INDEX_ROW_QUALIFIER, okey);
    conn.put(ociput, Order.TABLE_CUSTOMER_INDEX);

    // New-Order
    Put noput = new Put(okey);
    noput.add(Const.ID_FAMILY, NewOrder.NO_W_ID, wid);
    noput.add(Const.ID_FAMILY, NewOrder.NO_D_ID, did);
    noput.add(Const.ID_FAMILY, NewOrder.NO_O_ID, oid);
    conn.put(noput, NewOrder.TABLE);

    // Output-head
    output.append(String.format(LINE1, TITLE));
    output.append(String.format(LINE2, w_id(), d_id,
        Utils.timeToStringMinute(entry_d)));
    output.append(String.format(LINE3, c_id, c_last, c_credit,
        discount(c_discount)));
    output.append(String.format(LINE4, d_next_oid, ol_cnt, tax(w_tax),
        tax(d_tax)));
    output.append(LINE5);
    if (rbk != 1) output.append(LINE6);

    // For each order line
    long sum_amount = 0;
    for (int i = 0; i < ol_cnt; ++i) {
      // Item
      if (i_id[i] < 0) { // unused, rollback
        break;
      }
      byte[] ikey = Item.toRowkey(i_id[i]);
      Get iget = new Get(ikey);
      iget.addColumn(Const.NUMERIC_FAMILY, Item.I_PRICE);
      iget.addColumn(Const.TEXT_FAMILY, Item.I_NAME);
      iget.addColumn(Const.TEXT_FAMILY, Item.I_DATA);
      Result ires = conn.get(iget, Item.TABLE);
      long i_price = Utils.b2n(ires
          .getValue(Const.NUMERIC_FAMILY, Item.I_PRICE));
      String i_data = new String(ires.getValue(Const.TEXT_FAMILY, Item.I_DATA));
      String i_name = new String(ires.getValue(Const.TEXT_FAMILY, Item.I_NAME));

      // Stock-read
      byte[] s_w_id = Warehouse.toRowkey(supp_w_id[i]);
      byte[] skey = Stock.toRowkey(s_w_id, ikey);
      Get sget = new Get(skey);
      sget.addColumn(Const.TEXT_FAMILY, Stock.S_DIST[d_id]);
      sget.addColumn(Const.TEXT_FAMILY, Stock.S_DATA);
      sget.addColumn(Const.NUMERIC_FAMILY, Stock.S_QUANTITY);
      sget.addColumn(Const.NUMERIC_FAMILY, Stock.S_YTD);
      sget.addColumn(Const.NUMERIC_FAMILY, Stock.S_ORDER_CNT);
      sget.addColumn(Const.NUMERIC_FAMILY, Stock.S_REMOTE_CNT);
      Result sres = conn.get(sget, Stock.TABLE);
      long s_quantity = Utils.b2n(sres.getValue(Const.NUMERIC_FAMILY,
          Stock.S_QUANTITY));
      long s_ytd = Utils.b2n(sres.getValue(Const.NUMERIC_FAMILY, Stock.S_YTD));
      long s_order_cnt = Utils.b2n(sres.getValue(Const.NUMERIC_FAMILY,
          Stock.S_ORDER_CNT));
      long s_remote_cnt = Utils.b2n(sres.getValue(Const.NUMERIC_FAMILY,
          Stock.S_REMOTE_CNT));
      String s_data = new String(sres.getValue(Const.TEXT_FAMILY, Stock.S_DATA));
      byte[] s_dist = sres.getValue(Const.TEXT_FAMILY, Stock.S_DIST[d_id]);
      long ol_amount = ol_amount(quantity[i], i_price);
      sum_amount += ol_amount;
      // Stock-write
      Put sput = new Put(skey);
      if (s_quantity - quantity[i] >= 10) {
        s_quantity -= quantity[i];
      }
      else {
        s_quantity += (91 - quantity[i]);
      }
      ++s_ytd;
      ++s_order_cnt;
      if (supp_w_id[i] != w_id()) {
        ++s_remote_cnt;
        sput.add(Const.NUMERIC_FAMILY, Stock.S_REMOTE_CNT,
            Utils.n2b(s_remote_cnt));
      }
      sput.add(Const.NUMERIC_FAMILY, Stock.S_QUANTITY, Utils.n2b(s_quantity));
      sput.add(Const.NUMERIC_FAMILY, Stock.S_YTD, Utils.n2b(s_ytd));
      sput.add(Const.NUMERIC_FAMILY, Stock.S_ORDER_CNT, Utils.n2b(s_order_cnt));
      conn.put(sput, Stock.TABLE);

      // Order-Line
      byte[] olid = OrderLine.toOlid(i + 1);
      byte[] olkey = OrderLine.toRowkey(wid, did, oid, olid);
      Put olput = new Put(olkey);
      olput.add(Const.ID_FAMILY, OrderLine.OL_W_ID, wid);
      olput.add(Const.ID_FAMILY, OrderLine.OL_D_ID, did);
      olput.add(Const.ID_FAMILY, OrderLine.OL_O_ID, oid);
      olput.add(Const.ID_FAMILY, OrderLine.OL_NUMBER, olid);
      olput.add(Const.TEXT_FAMILY, OrderLine.OL_DIST_INFO, s_dist);
      olput.add(Const.TEXT_FAMILY, OrderLine.OL_I_ID, ikey);
      olput.add(Const.TEXT_FAMILY, OrderLine.OL_SUPPLY_W_ID, s_w_id);
      olput.add(Const.NUMERIC_FAMILY, OrderLine.OL_DELIVERY_D, null);
      olput.add(//
          Const.NUMERIC_FAMILY, OrderLine.OL_AMOUNT, Utils.n2b(ol_amount));
      olput.add(Const.NUMERIC_FAMILY, OrderLine.OL_QUANTITY,
          Utils.n2b(quantity[i]));
      conn.put(olput, OrderLine.TABLE);

      // Output-Line
      if (rbk == 1) continue;
      String bg;
      if (i_data.indexOf("ORIGINAL") != -1 && s_data.indexOf("ORIGINAL") != -1) {
        bg = "B";
      }
      else {
        bg = "G";
      }
      output.append(String.format(OL_LINE, supp_w_id[i], i_id[i], i_name,
          quantity[i], s_quantity, bg, price(i_price), amount(ol_amount)));
    }
    if (rbk == 1) {
      output.append(RBK_LINE);
      return;
    }
    double total_amount = total_amount(sum_amount, c_discount, w_tax, d_tax);
    output.append(String.format(SUM_LINE, total_amount));
  }

  private static double price(long price) {
    return price / 100.0;
  }

  private static double amount(long amount) {
    return amount / 100.0;
  }

  private static double tax(long tax) {
    return tax / 10000.0;
  }

  private static double discount(long disc) {
    return disc / 100.0;
  }

  /**
   * 
   * @param sum_amount
   * @param c_discount
   *          4,4
   * @param w_tax
   *          4,4
   * @param d_tax
   *          4,4
   * @return
   */
  private static double total_amount(long sum_amount, long c_discount,
      long w_tax, long d_tax) {
    return (sum_amount / 100.0) * (1 - c_discount / 10000.0)
        * (1 + w_tax / 10000.0 + d_tax / 10000.0);
  }

  /**
   * 
   * @param ol_quantity
   *          signed numeric(4)
   * @param i_price
   *          numeric(5, 2)
   * @return numeric(6, 2)
   */
  private static long ol_amount(long ol_quantity, long i_price) {
    return ol_quantity * i_price;
  }

  @Override
  public void generateInputData() {
    d_id = Utils.randomDid();
    c_id = Utils.NURand_C_ID();
    ol_cnt = Utils.random(5, 15);
    rbk = Utils.random(1, 100);
    entry_d = System.currentTimeMillis();
    i_id = new int[ol_cnt];
    supp_w_id = new int[ol_cnt];
    quantity = new int[ol_cnt];
    for (int i = 0; i < ol_cnt; ++i) {
      i_id[i] = Utils.NURand_OL_I_ID();
      quantity[i] = Utils.random(1, 10);
      int x = Utils.random(1, 100);
      if (Const.W == 1 || x > 1) {
        supp_w_id[i] = w_id();
      }
      else {
        all_local = 0;
        supp_w_id[i] = Utils.randomWidExcept(w_id());
      }
    }
    if (rbk == 1) {
      i_id[ol_cnt - 1] = -1;
    }
  }

  @Override
  public boolean shouldCommit() {
    return rbk != 1;
  }

  @Override
  public void waitForKeying() {
    Utils.sleep(18000);
  }

  @Override
  public void waitForThinking() {
    Utils.sleep(Utils.thinkingTime(12));
  }

  @Override
  public String getReportMessage() {
    return Character.toString(TpccTransaction.NEW_ORDER);
  }

  @Override
  public char getType() {
    return TpccTransaction.NEW_ORDER;
  }

}
