package ict.wde.hbase.tpcc.txn;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.tpcc.Const;
import ict.wde.hbase.tpcc.Utils;
import ict.wde.hbase.tpcc.table.District;
import ict.wde.hbase.tpcc.table.OrderLine;
import ict.wde.hbase.tpcc.table.Stock;
import ict.wde.hbase.tpcc.table.Warehouse;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class TStockLevel extends TpccTransaction {

  private static final String LINE1 = String.format("%29sStock-Level\n", "");
  private static final String LINE2 = "Warehouse: %05d  District: %02d\n";
  private static final String LINE3 = "\n";
  private static final String LINE4 = "Stock Level Threshold: %d\n";
  private static final String LINE5 = "\n";
  private static final String LINE6 = "Low Stock: %d\n";
  private static final String LINE7 = "\n";

  private int d_id;
  private int threshold;

  public TStockLevel(int w_id, HBaseConnection connection, int d_id) {
    super(w_id, connection);
    this.d_id = d_id;
  }

  @Override
  public void generateInputData() {
    threshold = Utils.random(10, 20);
  }

  @Override
  public void execute(HBaseConnection conn, StringBuffer output)
      throws IOException {
    byte[] wid = Warehouse.toRowkey(w_id());
    byte[] did = District.toDid(d_id);
    // District
    byte[] dkey = District.toRowkey(wid, did);
    Get dget = new Get(dkey);
    dget.addColumn(Const.NUMERIC_FAMILY, District.D_NEXT_O_ID);
    Result dres = conn.get(dget, District.TABLE);
    long d_next_o_id = Utils.b2n(dres.getValue(Const.NUMERIC_FAMILY,
        District.D_NEXT_O_ID));

    // Order-Line
    byte[] fromkey = OrderLine.toRowkey(wid, did, Utils.n2b(d_next_o_id - 20),
        OrderLine.MIN_OL_ID);
    byte[] tokey = OrderLine.toRowkey(wid, did, Utils.n2b(d_next_o_id - 1),
        OrderLine.MAX_OL_ID);
    Scan olscan = new Scan(fromkey, tokey);
    olscan.addColumn(Const.TEXT_FAMILY, OrderLine.OL_I_ID);
    ResultScanner olrs = conn.scan(olscan, OrderLine.TABLE);
    int lowCount = 0;
    for (Result olres : olrs) {
      // Stock
      byte[] ol_i_id = olres.getValue(Const.TEXT_FAMILY, OrderLine.OL_I_ID);
      byte[] skey = Stock.toRowkey(wid, ol_i_id);
      Get sget = new Get(skey);
      sget.addColumn(Const.NUMERIC_FAMILY, Stock.S_QUANTITY);
      Result sres = conn.get(sget, Stock.TABLE);
      long s_quantity = Utils.b2n(sres.getValue(Const.NUMERIC_FAMILY,
          Stock.S_QUANTITY));
      if (s_quantity < threshold) {
        ++lowCount;
      }
    }
    olrs.close();

    // Output
    output.append(LINE1);
    output.append(String.format(LINE2, w_id(), d_id));
    output.append(LINE3);
    output.append(String.format(LINE4, threshold));
    output.append(LINE5);
    output.append(String.format(LINE6, lowCount));
    output.append(LINE7);
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
    Utils.sleep(Utils.thinkingTime(5));
  }

  @Override
  public String getReportMessage() {
    return Character.toString(TpccTransaction.STOCK_LEVEL);
  }

  @Override
  public char getType() {
    return TpccTransaction.STOCK_LEVEL;
  }

}
