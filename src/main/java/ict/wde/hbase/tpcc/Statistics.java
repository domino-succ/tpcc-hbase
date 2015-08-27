package ict.wde.hbase.tpcc;

import ict.wde.hbase.tpcc.txn.TpccTransaction;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static java.lang.Long.max;
import static java.lang.Long.min;

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
            total += items[i].count.get();
        }
        for (int i = 0; i < items.length; ++i) {
            sb.append(items[i].toString(total)).append('\n');
        }
        return sb.toString();
    }

    final StatItem[] items = {new StatItem("NewOrder", 5000),
            new StatItem("Payment", 5000), new StatItem("OrderStatus", 5000),
            new StatItem("Delivery", 5000), new StatItem("StockLevel", 20000)};

    public void merge(StatItem[] s) {
        for (int i = 0; i < 5; i++) {
            items[i].merge(s[i]);
        }
    }

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
        return items[0].count.get() / minutes;
    }

    public double getTpmCPeroid() {
        double minutes = (System.currentTimeMillis() - periodTs) / 60000.0;
        if (minutes == 0) return 0;
        long count = items[0].count.get() - periodCount;
        if (minutes > PERIOD) {
            periodTs = System.currentTimeMillis();
            periodCount = items[0].count.get();
        }
        return count / minutes;
    }

    public void reset() {
        for (int i = 0; i < 5; i++) {
            items[i].reset();
        }
    }
}
