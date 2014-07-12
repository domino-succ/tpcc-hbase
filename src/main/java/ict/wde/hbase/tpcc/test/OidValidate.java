package ict.wde.hbase.tpcc.test;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.tpcc.Const;
import ict.wde.hbase.tpcc.Utils;
import ict.wde.hbase.tpcc.domino.DominoDriver;
import ict.wde.hbase.tpcc.table.District;
import ict.wde.hbase.tpcc.table.Order;
import ict.wde.hbase.tpcc.table.Warehouse;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class OidValidate {

  public static void main(String[] args) throws Exception {
    HBaseConnection conn = new DominoDriver().getConnection("p18:2181");
    conn.startTransaction();
    byte[] wid = Warehouse.toRowkey(120);
    byte[] did = District.toDid(0);
    Get dget = new Get(District.toRowkey(wid, did));
    dget.addColumn(Const.NUMERIC_FAMILY, District.D_NEXT_O_ID);
    Result dres = conn.get(dget, District.TABLE);
    long dnoid = Utils.b2n(dres.getValue(Const.NUMERIC_FAMILY,
        District.D_NEXT_O_ID));
    System.out.println("D_NEXT_O_ID = " + dnoid);

    byte[] okey1 = Order.toRowkey(wid, did, Order.toOid(0));
    byte[] okey2 = Order.toRowkey(wid, did, Order.toOid(Order.OID_UPPER_BOUND));
    Scan oscan = new Scan(okey1, okey2);
    oscan.addColumn(Const.ID_FAMILY, Order.O_ID);
    byte[] oid = null;
    ResultScanner ors = conn.scan(oscan, Order.TABLE);
    for (Result r : ors) {
      oid = r.getValue(Const.ID_FAMILY, Order.O_ID);
      System.out.println("Oid: " + new String(oid));
    }
    ors.close();
    long maxoid = Long.parseLong(new String(oid));
    System.out.println(dnoid + " v.s. " + maxoid);
    conn.commit();
    conn.close();
  }

}
