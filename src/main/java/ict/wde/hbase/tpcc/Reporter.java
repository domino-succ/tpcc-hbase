package ict.wde.hbase.tpcc;

import ict.wde.hbase.tpcc.domino.DominoDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by amosb on 2015-08-27.
 */
public class Reporter {

    private final String rpcAddr;
    private Statistics statistics;
    private CenterControl center;
    private Timer reportTimer;

    public Reporter(String addr) {
        rpcAddr = addr;
        statistics = new Statistics();
        reportTimer = new Timer(true);
    }

    public void update(char t, long a, long b, long c, long d) {
        statistics.stat(t, a, b, c, d);
    }

    public void start() throws IOException {
        String[] addr = rpcAddr.split(":");
        center = (CenterControl) RPC.getProxy(CenterControl.class, 0L,
                new InetSocketAddress(addr[0], Integer.parseInt(addr[1])),
                new Configuration());
        reportTimer.scheduleAtFixedRate(
                new TimerTask() {
                    public void run() {
                        try {
                            center.report(statistics.items);
                            statistics.reset();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }, 0, 1000);

    }
}
