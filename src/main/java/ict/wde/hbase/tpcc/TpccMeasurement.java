package ict.wde.hbase.tpcc;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.tpcc.domino.DominoDriver;
import ict.wde.hbase.tpcc.txn.TDeliveryExec;
import ict.wde.hbase.tpcc.txn.TpccTransaction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class TpccMeasurement implements CenterControl {

  private static String zkAddr;
  private static String outputPath;
  private static String rpcAddr;

  private final Delivery delivery = new Delivery();
  private HBaseConnection connection;
  private Statistics statistics = new Statistics();

  public static void main(String[] args) throws IOException {
    parseArgs(args);
    initOutputPath();
    TpccMeasurement measurement = new TpccMeasurement();
    measurement.openConnection();
    System.out.println("HBase connection established.");
    measurement.startTiming();
    // startTerminals();
    measurement.startReporting();
    System.out.println("Starting center service...");
    startService(measurement);
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return 0;
  }

  @Override
  public void reportMessage(String message) {
    String[] messageParts = message.split("\n");
    char t_type = messageParts[0].charAt(0);
    if (t_type == TpccTransaction.DELIVERY) {
      String delivery_msg = messageParts[1];
      delivery.enqueue(new DeliveryRequest(delivery_msg));
    }
  }

  @Override
  public void reportSummary(String summary) {
    String[] m = summary.split(" ");
    statistics.stat(m[0].charAt(0), Long.parseLong(m[1]), Long.parseLong(m[2]),
        Long.parseLong(m[3]), Long.parseLong(m[4]));
  }

  private void startTiming() {
    statistics.startTiming();
  }

  private void startReporting() {
    new Thread() {
      public void run() {
        while (true) {
          System.out.println(statistics.toString());
          System.out.println();
          try {
            Thread.sleep(10000);
          }
          catch (InterruptedException ie) {
            return;
          }
        }
      }
    }.start();
  }

  // private static void startTerminals() {
  // ProcessContact[][] pc = new ProcessContact[Const.W][10];
  // for (int w = 0; w < Const.W; ++w) {
  // for (int d = 0; d < 10; ++d) {
  // pc[w][d] = new ProcessContact(w, d);
  // }
  // }
  // for (int w = 0; w < Const.W; ++w) {
  // for (int d = 0; d < 10; ++d) {
  // pc[w][d].start();
  // }
  // }
  // }

  private void openConnection() throws IOException {
    connection = new DominoDriver().getConnection(zkAddr);
  }

  private static void initOutputPath() throws IOException {
    File path = new File(outputPath);
    if (path.exists() && !path.isDirectory()) {
      throw new IOException(outputPath + " isn't a directory.");
    }
    if (!path.exists()) {
      if (!path.mkdirs()) {
        throw new IOException("Could not initialize output directory: "
            + outputPath);
      }
    }
  }

  private static void parseArgs(String[] args) {
    zkAddr = args[0];
    outputPath = args[1];
    rpcAddr = args[2];
  }

  private static void startService(TpccMeasurement instance) throws IOException {
    String[] addr = rpcAddr.split(":");
    RPC.getServer(instance, addr[0], Integer.parseInt(addr[1]),
        new Configuration()).start();
  }

  private class Delivery implements Runnable {

    private LinkedList<DeliveryRequest> deliveryQueue = new LinkedList<DeliveryRequest>();
    private LinkedList<DeliveryRequest> workingQueue = new LinkedList<DeliveryRequest>();
    private boolean working = false;

    @Override
    public void run() {
      System.out.println("Delivery Queue Service running...");
      try {
        while (workingQueue.size() > 0) {
          DeliveryRequest req = workingQueue.poll();
          for (int d_id = 0; d_id < 10; ++d_id) {
            TpccTransaction t = new TDeliveryExec(req.w_id, connection, d_id,
                req.carrier_id, req.delivery_d);
            String output = t.execute();
            if (output == null) continue;
            output(String.format("%s\t%s\t%s",
                Utils.timeToStringSecond(req.queued_d), output,
                Utils.timeToStringSecond(System.currentTimeMillis())));
          }
        }
      }
      finally {
        working = false;
      }
      synchronized (deliveryQueue) {
        trigger();
      }
    }

    void enqueue(DeliveryRequest req) {
      synchronized (deliveryQueue) {
        deliveryQueue.add(req);
        trigger();
      }
    }

    void trigger() {
      if (working || deliveryQueue.size() == 0) return;
      working = true;
      workingQueue.addAll(deliveryQueue);
      deliveryQueue.clear();
      new Thread(this).start();
    }

    private PrintWriter writer = null;

    private void output(String s) {
      try {
        if (writer == null) {
          writer = new PrintWriter(new FileWriter(String.format(
              "%s/delivery.log", outputPath)));
        }
        writer.println(s);
        writer.flush();
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  // static class ProcessContact extends Thread {
  //
  // static final String TERMINAL_CLASS = "ict.wde.hbase.tpcc.Terminal";
  //
  // final int w_id;
  // final int sl_d_id;
  // final ProcessBuilder builder;
  //
  // ProcessContact(int w_id, int sl_d_id) {
  // this.w_id = w_id;
  // this.sl_d_id = sl_d_id;
  // builder = new ProcessBuilder("java");
  // }
  //
  // void initBuilder() {
  // Properties props = System.getProperties();
  // for (Object key : props.keySet()) {
  // builder.command().add(
  // String.format("-D%s=%s", key.toString(), props.get(key)));
  // }
  // builder.command().add(String.format("-DC_C_LAST=%d", Const.C_C_LAST_RUN));
  // builder.command().add(String.format("-DC_C_ID=%d", Const.C_C_ID_RUN));
  // builder.command().add(
  // String.format("-DC_OL_I_ID=%d", Const.C_OL_I_ID_RUN));
  // builder.command().add(TERMINAL_CLASS);
  // builder.command().add(Integer.toString(w_id));
  // builder.command().add(Integer.toString(sl_d_id));
  // builder.command().add(zkAddr);
  // builder.command().add(
  // String.format("%s/%05d-%02d.log", outputPath, w_id, sl_d_id));
  // builder.redirectError(new File(String.format("%s/%05d-%02d.err",
  // outputPath, w_id, sl_d_id)));
  // builder.redirectOutput(Redirect.PIPE);
  // }
  //
  // public void run() {
  // Process child = null;
  // try {
  // child = builder.start();
  // }
  // catch (IOException ioe) {
  // ioe.printStackTrace();
  // return;
  // }
  // BufferedReader reader = new BufferedReader(new InputStreamReader(
  // child.getInputStream()));
  // String message;
  // try {
  // while ((message = reader.readLine()) != null) {
  // char t_type = message.charAt(0);
  // if (t_type == TpccTransaction.DELIVERY) {
  // String delivery_msg = reader.readLine();
  // delivery.enqueue(new DeliveryRequest(delivery_msg));
  // }
  // summary(t_type, reader.readLine());
  // }
  // }
  // catch (Throwable t) {
  // System.err.println(String.format("Process %05d-%02d got an error: ",
  // w_id, sl_d_id));
  // t.printStackTrace();
  // }
  // finally {
  // try {
  // reader.close();
  // }
  // catch (IOException e) {
  // }
  // }
  // }
  // }

  static class DeliveryRequest {
    final int w_id;
    final int carrier_id;
    final long delivery_d;
    final long queued_d;

    DeliveryRequest(String message) {
      String[] m = message.split(" ");
      w_id = Integer.parseInt(m[0]);
      carrier_id = Integer.parseInt(m[1]);
      delivery_d = Long.parseLong(m[2]);
      queued_d = System.currentTimeMillis();
    }
  }

}
