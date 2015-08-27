package ict.wde.hbase.tpcc.txn;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.tpcc.Utils;
import ict.wde.hbase.tpcc.domino.DominoDriver;

import java.io.IOException;

public abstract class TpccTransaction {

  public static final char NEW_ORDER = 'N';
  public static final char PAYMENT = 'P';
  public static final char ORDER_STATUS = 'O';
  public static final char DELIVERY = 'D';
  public static final char STOCK_LEVEL = 'S';

  private final int w_id;
  private final HBaseConnection connection;

  public TpccTransaction(int w_id, HBaseConnection connection) {
    this.w_id = w_id;
    this.connection = connection;
    generateInputData();
  }

  public int w_id() {
    return this.w_id;
  }

  public HBaseConnection connection() {
    return this.connection;
  }

  public final String execute() {
    StringBuffer output = new StringBuffer(1024);
    HBaseConnection conn = this.connection();
    int retryCount = 0;
    do {
      try {
        conn.startTransaction();
        execute(conn, output);
        if (shouldCommit()) conn.commit();
        else conn.rollback();
        break;
      }
      catch (IOException e) {
        ++retryCount;
        if (retryCount % 5 == 0) {
          System.err.println("Retry " + retryCount + ": " + e.toString());
        }
        try {
          conn.rollback();
        }
        catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }
      catch (Exception e) {
        ++retryCount;
        if (retryCount % 5 == 0) {
          System.err.println("Retry " + retryCount + ": " + e.toString());
          e.printStackTrace();
        }
        try {
          conn.rollback();
        }
        catch (IOException ioe) {
          ioe.printStackTrace();
        }
        Utils.sleep(Utils.random(1000, 2000));
      }
      if (retryCount > 20) {
        return null;
      }
    }
    while (true);
    return output.toString();
  }

  public abstract void generateInputData();

  public abstract void execute(HBaseConnection conn, StringBuffer output)
      throws IOException;

  public abstract boolean shouldCommit();

  public abstract void waitForKeying();

  public abstract void waitForThinking();

  // For measurement center
  public abstract String getReportMessage();

  public abstract char getType();

  public static void main(String[] args) throws IOException {
    HBaseConnection conn = new DominoDriver().getConnection("p18:2181");
    TpccTransaction t = new TDeliveryExec(6, conn, 1, 2,
        System.currentTimeMillis());
    String output = t.execute();
    conn.close();
    System.out.println(output);
  }
}
