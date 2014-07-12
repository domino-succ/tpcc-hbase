package ict.wde.hbase.tpcc;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.tpcc.domino.DominoDriver;
import ict.wde.hbase.tpcc.txn.TDelivery;
import ict.wde.hbase.tpcc.txn.TNewOrder;
import ict.wde.hbase.tpcc.txn.TOrderStatus;
import ict.wde.hbase.tpcc.txn.TPayment;
import ict.wde.hbase.tpcc.txn.TStockLevel;
import ict.wde.hbase.tpcc.txn.TpccTransaction;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class Terminal extends Thread {

  private final int w_id;
  private final int sl_d_id;
  private CenterControl center;
  private String outputPath;
  private HBaseConnection connection;

  public Terminal(int w_id, int sl_d_id, String outputPath) {
    this.w_id = w_id;
    this.sl_d_id = sl_d_id;
    this.outputPath = outputPath;
  }

  public void run() {
    try {
      openConnection();
      while (true) {
        long t1 = System.currentTimeMillis();
        TpccTransaction t = getTransaction();
        // Parent contact 1 (trasaction type & message)
        System.out.println(sl_d_id + " - Before Executing: "
            + t.getReportMessage());
        center.reportMessage(t.getReportMessage());
        long t2 = System.currentTimeMillis();
        t.waitForKeying();
        long t3 = System.currentTimeMillis();
        String output = t.execute();
        if (output == null) {
          continue;
        }
        output(output);
        long t4 = System.currentTimeMillis();
        t.waitForThinking();
        long t5 = System.currentTimeMillis();
        // Parent contact 2 (summary)
        tellParent(t, t2 - t1, t3 - t2, t4 - t3, t5 - t4);
      }
    }
    catch (Throwable t) {
      t.printStackTrace();
    }
    finally {
      closeConnection();
      closeWriter();
    }
  }

  private void tellParent(TpccTransaction t, long menuRT, long keyingTime,
      long txnRT, long thinkingTime) {
    String message = String.format("%c %d %d %d %d", t.getType(), menuRT,
        keyingTime, txnRT, thinkingTime);
    System.out.println(sl_d_id + " - " + message);
    center.reportSummary(message);
  }

  private void closeWriter() {
    if (writer == null) return;
    try {
      writer.close();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void closeConnection() {
    if (connection == null) return;
    try {
      connection.close();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  private PrintWriter writer = null;

  private void output(String s) {
    try {
      if (writer == null) {
        writer = new PrintWriter(new FileWriter(outputPath));
      }
      writer.println(s);
      writer.flush();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void openConnection() throws IOException {
    connection = new DominoDriver().getConnection(zkAddr);
    String[] addr = rpcAddr.split(":");
    center = (CenterControl) RPC.getProxy(CenterControl.class, 0L,
        new InetSocketAddress(addr[0], Integer.parseInt(addr[1])),
        new Configuration());
  }

  private ArrayList<Character> deck = new ArrayList<Character>(23);
  private int deckI = 0;

  private TpccTransaction getTransaction() {
    if (deck.size() == 0) {
      for (int i = 0; i < 10; ++i) {
        deck.add(TpccTransaction.NEW_ORDER);
        deck.add(TpccTransaction.PAYMENT);
      }
      deck.add(TpccTransaction.ORDER_STATUS);
      deck.add(TpccTransaction.DELIVERY);
      deck.add(TpccTransaction.STOCK_LEVEL);
    }
    if (deckI == 0) {
      Collections.shuffle(deck);
    }
    try {
      char type = deck.get(deckI);
      switch (type) {
      case TpccTransaction.NEW_ORDER:
        return new TNewOrder(w_id, connection);
      case TpccTransaction.PAYMENT:
        return new TPayment(w_id, connection);
      case TpccTransaction.ORDER_STATUS:
        return new TOrderStatus(w_id, connection);
      case TpccTransaction.DELIVERY:
        return new TDelivery(w_id, connection);
      case TpccTransaction.STOCK_LEVEL:
      default:
        return new TStockLevel(w_id, connection, sl_d_id);
      }
    }
    finally {
      deckI = (deckI + 1) % deck.size();
    }
  }

  private static int[] wRange;
  private static String zkAddr;
  private static String rpcAddr;
  private static String outputDir;

  private static void parseArgs(String[] args) {
    wRange = new int[2];
    String[] range = args[0].split("-");
    wRange[0] = Integer.parseInt(range[0]);
    if (range.length == 1) {
      wRange[1] = wRange[0];
    }
    else {
      wRange[1] = Integer.parseInt(range[1]);
    }
    zkAddr = args[1];
    outputDir = args[2];
    rpcAddr = args[3];
  }

  public static void main(String[] args) throws Exception {
    parseArgs(args);
    for (int w = wRange[0]; w <= wRange[1]; ++w) {		
      for (int d = 0; d < 10; ++d) {
        Terminal t = new Terminal(w, d, String.format("%s/%d-%d.log",
            outputDir, w, d));
        t.start();
      }
    }
  }
}
