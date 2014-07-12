package ict.wde.hbase.tpcc.txn;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.tpcc.Const;
import ict.wde.hbase.tpcc.Utils;
import ict.wde.hbase.tpcc.table.Customer;
import ict.wde.hbase.tpcc.table.District;
import ict.wde.hbase.tpcc.table.NewOrder;
import ict.wde.hbase.tpcc.table.Order;
import ict.wde.hbase.tpcc.table.OrderLine;
import ict.wde.hbase.tpcc.table.Warehouse;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class TDeliveryExec extends TpccTransaction {

  private static final String LINE1 = "%s\t%02d\t%s\t%s";

  private int carrier_id;
  private long delivery_d;
  private int d_id;

  public TDeliveryExec(int w_id, HBaseConnection connection, int d_id,
      int carrier_id, long delivery_d) {
    super(w_id, connection);
    this.carrier_id = carrier_id;
    this.delivery_d = delivery_d;
    this.d_id = d_id;
  }

  @Override
  public void generateInputData() {
  }

  @Override
  public void execute(HBaseConnection conn, StringBuffer output)
      throws IOException {

    // New-Order-find earliest
    byte[] wid = Warehouse.toRowkey(w_id());
    byte[] did = District.toDid(d_id);
    byte[] nofromkey = NewOrder.toRowkey(wid, did, Order.OID_LOWER_BOUND_B);
    byte[] notokey = NewOrder.toRowkey(wid, did, Order.OID_UPPER_BOUND_B);
    Scan noscan = new Scan(nofromkey, notokey);
    noscan.addColumn(Const.ID_FAMILY, NewOrder.NO_O_ID);
    ResultScanner nors = conn.scan(noscan, NewOrder.TABLE);
    Result nores = nors.next();
    nors.close();
    if (nores == null || nores.isEmpty()) {
      output.append(String.format(LINE1, new String(wid), this.carrier_id,
          new String(did), "SKIPPED"));
      return;
    }
    // New-Order-delete
    byte[] oid = nores.getValue(Const.ID_FAMILY, NewOrder.NO_O_ID);
    byte[] nokey = NewOrder.toRowkey(wid, did, oid);
    conn.delete(nokey, NewOrder.TABLE);

    // Order-read
    Get oget = new Get(nokey);
    oget.addColumn(Const.TEXT_FAMILY, Order.O_C_ID);
    Result ores = conn.get(oget, Order.TABLE);
    byte[] cid = ores.getValue(Const.TEXT_FAMILY, Order.O_C_ID);
    // Order-write
    byte[] carrierid = String.format("%02d", carrier_id).getBytes();
    Put oput = new Put(nokey);
    oput.add(Const.TEXT_FAMILY, Order.O_CARRIER_ID, carrierid);
    conn.put(oput, Order.TABLE);

    // Order-Line-rw
    byte[][] olrange = OrderLine.toOrderQuery(wid, did, oid);
    Scan olscan = new Scan(olrange[0], olrange[1]);
    olscan.addColumn(Const.NUMERIC_FAMILY, OrderLine.OL_AMOUNT);
    ResultScanner olrs = conn.scan(olscan, OrderLine.TABLE);
    byte[] deliveryd = Utils.n2b(delivery_d);
    long sum_amount = 0;
    for (Result olres : olrs) {
      sum_amount += Utils.b2n(olres.getValue(Const.NUMERIC_FAMILY,
          OrderLine.OL_AMOUNT));
      Put olput = new Put(olres.getRow());
      olput.add(Const.NUMERIC_FAMILY, OrderLine.OL_DELIVERY_D, deliveryd);
      conn.put(olput, OrderLine.TABLE);
    }
    olrs.close();

    // Customer-rw
    byte[] ckey = Customer.toRowkey(wid, did, cid);
    Get cget = new Get(ckey);
    cget.addColumn(Const.NUMERIC_FAMILY, Customer.C_BALANCE);
    cget.addColumn(Const.NUMERIC_FAMILY, Customer.C_DELIVERY_CNT);
    Result cres = conn.get(cget, Customer.TABLE);
    long c_balance = Utils.b2n(cres.getValue(Const.NUMERIC_FAMILY,
        Customer.C_BALANCE));
    long c_delivery_cnt = Utils.b2n(cres.getValue(Const.NUMERIC_FAMILY,
        Customer.C_DELIVERY_CNT));
    Put cput = new Put(ckey);
    cput.add(Const.NUMERIC_FAMILY, Customer.C_BALANCE,
        Utils.n2b(c_balance + sum_amount));
    cput.add(Const.NUMERIC_FAMILY, Customer.C_DELIVERY_CNT,
        Utils.n2b(c_delivery_cnt + 1));
    conn.put(cput, Customer.TABLE);

    output.append(String.format(LINE1, new String(wid), this.carrier_id,
        new String(did), new String(oid)));
  }

  @Override
  public boolean shouldCommit() {
    return true;
  }

  @Override
  public void waitForKeying() {
  }

  @Override
  public void waitForThinking() {
  }

  @Override
  public String getReportMessage() {
    return null;
  }

  @Override
  public char getType() {
    return 0;
  }

}
