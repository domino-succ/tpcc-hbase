package ict.wde.hbase.tpcc;

import ict.wde.hbase.tpcc.txn.TpccTransaction;

public class Statistics {

  @Override
  public String toString() {
    long total = 0;
    StringBuffer sb = new StringBuffer(256);
    sb.append("==================== Statistics ====================\n");
    sb.append(String
        .format(
            "tpmC Global: %1.2f, tpmC Period: %1.2f, Measurement Time: %s, Reporting time: %s\n",
            getTpmCGlobal(), getTpmCPeroid(),
            Utils.timeToStringDuration(System.currentTimeMillis() - startTs),
            Utils.timeToStringSecond(System.currentTimeMillis())));
    for (int i = 0; i < items.length; ++i) {
      total += items[i].count;
    }
    for (int i = 0; i < items.length; ++i) {
      sb.append(items[i].toString(total)).append('\n');
    }
    return sb.toString();
  }

  static String per(double n1, double n2) {
    if (n2 == 0) return "NaN";
    return String.format("%1.2f%%", n1 * 100.0 / n2);
  }

  static double sec(long ts) {
    return ts / 1000.0;
  }

  static class StatItem {

    private long count = 0;
    private long timedoutCount = 0;
    private long menuRT = 0;
    private long menuRTMax = 0;
    private long menuRTMin = Long.MAX_VALUE;
    private long keyingTime = 0;
    private long keyingTimeMax = 0;
    private long keyingTimeMin = Long.MAX_VALUE;
    private long txnRT = 0;
    private long txnRTMax = 0;
    private long txnRTMin = Long.MAX_VALUE;
    private long thinkingTime = 0;
    private long thinkingTimeMax = 0;
    private long thinkingTimeMin = Long.MAX_VALUE;

    private final long timedoutThreshold;
    private final String name;

    public StatItem(String name, long timedoutThreshold) {
      this.name = name;
      this.timedoutThreshold = timedoutThreshold;
    }

    public String toString(double total) {
      StringBuffer sb = new StringBuffer(128);
      sb.append(String.format("%s: %d, %s, Timed out: %d, %s\n", name, count,
          per(count, total), timedoutCount, per(timedoutCount, count)));
      sb.append(String.format(
          "    menuRT: Avg = %1.2f, Max = %1.2f, Min = %1.2f\n", sec(menuRT)
              / count, sec(menuRTMax), sec(menuRTMin)));
      sb.append(String.format(
          "    keyingTime: Avg = %1.2f, Max = %1.2f, Min = %1.2f\n",
          sec(keyingTime) / count, sec(keyingTimeMax), sec(keyingTimeMin)));
      sb.append(String.format(
          "    txnRT: Avg = %1.2f, Max = %1.2f, Min = %1.2f\n", sec(txnRT)
              / count, sec(txnRTMax), sec(txnRTMin)));
      sb.append(String
          .format("    thinkingTime: Avg = %1.2f, Max = %1.2f, Min = %1.2f",
              sec(thinkingTime) / count, sec(thinkingTimeMax),
              sec(thinkingTimeMin)));
      return sb.toString();
    }

    public synchronized void stat(long menuRT, long keyingTime, long txnRT,
        long thinkingTime) {
      ++count;
      if (txnRT > timedoutThreshold) {
        ++timedoutCount;
      }
      if (menuRT > menuRTMax) {
        menuRTMax = menuRT;
      }
      if (menuRT < menuRTMin) {
        menuRTMin = menuRT;
      }
      this.menuRT += menuRT;
      if (keyingTime > keyingTimeMax) {
        keyingTimeMax = keyingTime;
      }
      if (keyingTime < keyingTimeMin) {
        keyingTimeMin = keyingTime;
      }
      this.keyingTime += keyingTime;
      if (txnRT > txnRTMax) {
        txnRTMax = txnRT;
      }
      if (txnRT < txnRTMin) {
        txnRTMin = txnRT;
      }
      this.txnRT += txnRT;
      if (thinkingTime > thinkingTimeMax) {
        thinkingTimeMax = thinkingTime;
      }
      if (thinkingTime < thinkingTimeMin) {
        thinkingTimeMin = thinkingTime;
      }
      this.thinkingTime += thinkingTime;
    }
  }

  final StatItem[] items = { new StatItem("NewOrder", 5000),
      new StatItem("Payment", 5000), new StatItem("OrderStatus", 5000),
      new StatItem("Delivery", 5000), new StatItem("StockLevel", 20000) };

  public void stat(char t_type, long menuRT, long keyingTime, long txnRT,
      long thinkingTime) {
    switch (t_type) {
    case TpccTransaction.NEW_ORDER:
      items[0].stat(menuRT, keyingTime, txnRT, thinkingTime);
      break;
    case TpccTransaction.PAYMENT:
      items[1].stat(menuRT, keyingTime, txnRT, thinkingTime);
      break;
    case TpccTransaction.ORDER_STATUS:
      items[2].stat(menuRT, keyingTime, txnRT, thinkingTime);
      break;
    case TpccTransaction.DELIVERY:
      items[3].stat(menuRT, keyingTime, txnRT, thinkingTime);
      break;
    case TpccTransaction.STOCK_LEVEL:
    default:
      items[4].stat(menuRT, keyingTime, txnRT, thinkingTime);
      break;
    }
  }

  private long startTs = System.currentTimeMillis();
  private long periodTs = System.currentTimeMillis();
  private long periodCount = 0;

  static final double PERIOD = 5; // minutes

  public void startTiming() {
    startTs = System.currentTimeMillis();
  }

  public double getTpmCGlobal() {
    double minutes = (System.currentTimeMillis() - startTs) / 60000.0;
    if (minutes == 0) return 0;
    return items[0].count / minutes;
  }

  public double getTpmCPeroid() {
    double minutes = (System.currentTimeMillis() - periodTs) / 60000.0;
    if (minutes == 0) return 0;
    long count = items[0].count - periodCount;
    if (minutes > PERIOD) {
      periodTs = System.currentTimeMillis();
      periodCount = items[0].count;
    }
    return count / minutes;
  }

}
