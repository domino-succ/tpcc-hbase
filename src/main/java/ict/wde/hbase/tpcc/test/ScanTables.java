package ict.wde.hbase.tpcc.test;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.tpcc.domino.DominoDriver;
import ict.wde.hbase.tpcc.table.Warehouse;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import static org.apache.log4j.Logger.*;

public class ScanTables {
  public static void main(String[] args) throws Exception {

//    getRootLogger().setLevel(Level.WARN);
    HBaseConnection conn = new DominoDriver().getConnection("nobida143:2181");
    conn.startTransaction();

    ResultScanner rs = conn.scan(new Scan(), Warehouse.TABLE);
    for (Result r : rs) {
      System.out.println(new String(r.getRow()));
    }
    rs.close();
    conn.commit();
    conn.close();
  }
}
