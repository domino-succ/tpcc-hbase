package ict.wde.hbase.tpcc;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Long.max;
import static java.lang.Long.min;

/**
 * Created by amosb on 2015-08-27.
 */
public class StatItem implements Writable {

    public AtomicLong count = new AtomicLong(0);
    public AtomicLong timedoutCount = new AtomicLong(0);
    public AtomicLong menuRT = new AtomicLong(0);
    public AtomicLong menuRTMax = new AtomicLong(0);
    public AtomicLong menuRTMin = new AtomicLong(Long.MAX_VALUE);
    public AtomicLong keyingTime = new AtomicLong(0);
    public AtomicLong keyingTimeMax = new AtomicLong(0);
    public AtomicLong keyingTimeMin = new AtomicLong(Long.MAX_VALUE);
    public AtomicLong txnRT = new AtomicLong(0);
    public AtomicLong txnRTMax = new AtomicLong(0);
    public AtomicLong txnRTMin = new AtomicLong(Long.MAX_VALUE);
    public AtomicLong thinkingTime = new AtomicLong(0);
    public AtomicLong thinkingTimeMax = new AtomicLong(0);
    public AtomicLong thinkingTimeMin = new AtomicLong(Long.MAX_VALUE);

    private long timedoutThreshold;
    private String name;

    //Reflection needs this!!
    public StatItem() {}

    public StatItem(String name, long timedoutThreshold) {
        this.name = name;
        this.timedoutThreshold = timedoutThreshold;
    }

    static String per(double n1, double n2) {
        if (n2 == 0) return "NaN";
        return String.format("%1.2f%%", n1 * 100.0 / n2);
    }

    static double sec(long ts) {
        return ts / 1000.0;
    }
    public String toString(double total) {
        StringBuffer sb = new StringBuffer(128);
        sb.append(String.format("%s: %d, %s, Timed out: %d, %s\n", name, count.get(),
                per(count.get(), total), timedoutCount.get(), per(timedoutCount.get(), count.get())));
        sb.append(String.format(
                "    menuRT: Avg = %1.2f, Max = %1.2f, Min = %1.2f\n", sec(menuRT.get())
                        / count.get(), sec(menuRTMax.get()), sec(menuRTMin.get())));
        sb.append(String.format(
                "    keyingTime: Avg = %1.2f, Max = %1.2f, Min = %1.2f\n",
                sec(keyingTime.get()) / count.get(), sec(keyingTimeMax.get()), sec(keyingTimeMin.get())));
        sb.append(String.format(
                "    txnRT: Avg = %1.2f, Max = %1.2f, Min = %1.2f\n", sec(txnRT.get())
                        / count.get(), sec(txnRTMax.get()), sec(txnRTMin.get())));
        sb.append(String
                .format("    thinkingTime: Avg = %1.2f, Max = %1.2f, Min = %1.2f",
                        sec(thinkingTime.get()) / count.get(), sec(thinkingTimeMax.get()),
                        sec(thinkingTimeMin.get())));
        return sb.toString();
    }

    static void maxSet(long v, AtomicLong c) {
        while (v > c.get()) {
            v = c.getAndSet(v);
        }
    }

    static void minSet(long v, AtomicLong c) {
        while (v < c.get()) {
            v = c.getAndSet(v);
        }
    }
    public synchronized void merge(StatItem s) {
        count.addAndGet(s.count.get());
        timedoutCount.addAndGet(s.timedoutCount.get());
        menuRT.addAndGet(s.menuRT.get());
        keyingTime.addAndGet(s.keyingTime.get());
        txnRT.addAndGet(s.txnRT.get());
        thinkingTime.addAndGet(s.thinkingTime.get());

        maxSet(s.menuRTMax.get(), menuRTMax);
        minSet(s.menuRTMin.get(), menuRTMin);
        maxSet(s.keyingTimeMax.get(), keyingTimeMax);
        minSet(s.keyingTimeMin.get(), keyingTimeMin);
        maxSet(s.txnRTMax.get(), txnRTMax);
        minSet(s.txnRTMin.get(), txnRTMin);
        maxSet(s.thinkingTimeMax.get(), thinkingTimeMax);
        minSet(s.thinkingTimeMin.get(), thinkingTimeMin);
    }

    public synchronized void stat(long menuRT, long keyingTime, long txnRT,
                                  long thinkingTime) {
        count.addAndGet(1);
        if (txnRT > timedoutThreshold) {
            timedoutCount.addAndGet(1);
        }

        maxSet(menuRT, menuRTMax);
        minSet(menuRT, menuRTMin);
        maxSet(keyingTime, keyingTimeMax);
        minSet(keyingTime, keyingTimeMin);
        maxSet(txnRT, txnRTMax);
        minSet(txnRT, txnRTMin);
        maxSet(thinkingTime, thinkingTimeMax);
        minSet(thinkingTime, thinkingTimeMin);
        this.menuRT.addAndGet(menuRT);
        this.keyingTime.addAndGet(keyingTime);
        this.txnRT.addAndGet(txnRT);
        this.thinkingTime.addAndGet(thinkingTime);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(count.get());
        out.writeLong(timedoutCount.get());
        out.writeLong(menuRT.get());
        out.writeLong(keyingTime.get());
        out.writeLong(txnRT.get());
        out.writeLong(thinkingTime.get());
        out.writeLong(menuRTMax.get());
        out.writeLong(menuRTMin.get());
        out.writeLong(keyingTimeMax.get());
        out.writeLong(keyingTimeMin.get());
        out.writeLong(txnRTMax.get());
        out.writeLong(txnRTMin.get());
        out.writeLong(thinkingTimeMax.get());
        out.writeLong(thinkingTimeMin.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count.set(in.readLong());
        timedoutCount.set(in.readLong());
        menuRT.set(in.readLong());
        keyingTime.set(in.readLong());
        txnRT.set(in.readLong());
        thinkingTime.set(in.readLong());
        menuRTMax.set(in.readLong());
        menuRTMin.set(in.readLong());
        keyingTimeMax.set(in.readLong());
        keyingTimeMin.set(in.readLong());
        txnRTMax.set(in.readLong());
        txnRTMin.set(in.readLong());
        thinkingTimeMax.set(in.readLong());
        thinkingTimeMin.set(in.readLong());
    }

    public void reset() {
        count.set(0);
        timedoutCount.set(0);
        menuRT.set(0);
        keyingTime.set(0);
        txnRT.set(0);
        thinkingTime.set(0);
    }
}
