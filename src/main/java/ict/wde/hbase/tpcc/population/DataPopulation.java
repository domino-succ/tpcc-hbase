package ict.wde.hbase.tpcc.population;

import ict.wde.domino.client.Domino;
import ict.wde.hbase.tpcc.Const;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class DataPopulation {

  public static final int POP_W_FROM = Integer.parseInt(System.getProperty(
      "POP_W_FROM", "0"));
  public static final int POP_W_TO = Integer.parseInt(System.getProperty(
      "POP_W_TO", Integer.toString(Const.W - 1)));

  private final Configuration conf;

  static final int BATCH_OP = 100;
  private final Map<byte[], HTableInterface> tables = new TreeMap<byte[], HTableInterface>(
      Bytes.BYTES_COMPARATOR);

  private HTableInterface getTable(byte[] name) throws IOException {
    synchronized (tables) {
      HTableInterface table = tables.get(name);
      if (table == null) {
        table = new HTable(conf, name);
        tables.put(name, table);
      }
      return table;
    }
  }

  public DataPopulation(Configuration conf) {
    this.conf = conf;
  }

  // public HBaseConnection connection() {
  // return this.connection;
  // }

  public void put(Put put, byte[] table) throws IOException {
    Domino.loadData(put, getTable(table));
  }

  public void startPopSync() throws IOException {
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
          break;
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
    }
    while (true);
    closeAllTables();
    System.out.println(this.getClass().getName() + " population complete.");
  }

  private void closeAllTables() {
    for (Map.Entry<byte[], HTableInterface> ent : tables.entrySet()) {
      try {
        ent.getValue().close();
      }
      catch (IOException ioe) {
      }
    }
    tables.clear();
  }

  public abstract int popOneRow() throws IOException;

}
