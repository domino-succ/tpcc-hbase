package ict.wde.hbase.tpcc.population;

import ict.wde.domino.client.Domino;
import ict.wde.hbase.tpcc.Const;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class DataPopulation implements Callable<Integer> {

  public static final int POP_W_FROM = Integer.parseInt(System.getProperty(
      "POP_W_FROM", "0"));
  public static final int POP_W_TO = Integer.parseInt(System.getProperty(
      "POP_W_TO", Integer.toString(Const.W - 1)));

  static final int BATCH_OP = 1000;

  public void put(Put put, HTableInterface table) throws IOException {
    Domino.loadData(put, table);
  }

  public Integer call() throws IOException {
    System.out.println("Starting " + this.getClass().getName()
        + " population...");
    int count;
    long total = 0;
    int batch = 0;
    do {
      try {
        count = popOneRow();
      }
      catch (IOException ioe) {
        System.err.println(ioe.toString());
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException ie) {
          return -1;
        }
        continue;
      }
      if (count <= 0) {
        break;
      }
      batch += count;
      total += count;
      if (batch >= BATCH_OP) {
        batch = 0;
        System.out.println(total + " rows written.");
      }
    } while(true);
    System.out.println(this.getClass().getName() + " population complete.");
    return 0;
  }

  public abstract int popOneRow() throws IOException;

}
