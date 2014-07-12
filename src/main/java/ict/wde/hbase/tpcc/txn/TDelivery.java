package ict.wde.hbase.tpcc.txn;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.tpcc.Utils;

import java.io.IOException;

public class TDelivery extends TpccTransaction {

  private static final String LINE1 = String.format("%32sDelivery\n", "");
  private static final String LINE2 = "Warehouse: %05d\n";
  private static final String LINE3 = "\n";
  private static final String LINE4 = "Carrier Number: %02d\n";
  private static final String LINE5 = "\n";
  private static final String LINE6 = "Execution Status: Delivery has been queued\n";
  private static final String LINE7 = "\n";

  private int o_carrier_id;
  private long ol_delivery_d;

  public TDelivery(int w_id, HBaseConnection connection) {
    super(w_id, connection);
  }

  @Override
  public void generateInputData() {
    o_carrier_id = Utils.random(1, 10);
    ol_delivery_d = System.currentTimeMillis();
  }

  @Override
  public void execute(HBaseConnection conn, StringBuffer output)
      throws IOException {
    // Output
    output.append(LINE1);
    output.append(String.format(LINE2, w_id()));
    output.append(LINE3);
    output.append(String.format(LINE4, o_carrier_id));
    output.append(LINE5);
    output.append(LINE6);
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
    return String.format("%c\n%d %d %d", TpccTransaction.DELIVERY, w_id(),
        o_carrier_id, ol_delivery_d);
  }

  @Override
  public char getType() {
    return TpccTransaction.DELIVERY;
  }

}
