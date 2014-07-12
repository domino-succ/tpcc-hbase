package ict.wde.hbase.tpcc.population;

import ict.wde.hbase.tpcc.Const;
import ict.wde.hbase.tpcc.Utils;
import ict.wde.hbase.tpcc.table.Item;
import ict.wde.hbase.tpcc.table.Stock;
import ict.wde.hbase.tpcc.table.Warehouse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class StockPop extends DataPopulation {

  static final long POP_TOTAL_ID = ItemPop.POP_TOTAL_ID;
  private int wid = POP_W_FROM;
  private int iid = 0;

  public StockPop(Configuration connection) {
    super(connection);
  }

  @Override
  public int popOneRow() throws IOException {
    if (wid > POP_W_TO && iid >= POP_TOTAL_ID) return 0;
    byte[] w_id = Warehouse.toRowkey(wid);
    byte[] i_id = Item.toRowkey(iid);
    Put put = new Put(Stock.toRowkey(w_id, i_id));
    put.add(Const.ID_FAMILY, Stock.S_W_ID, w_id);
    put.add(Const.ID_FAMILY, Stock.S_I_ID, i_id);
    put.add(Const.NUMERIC_FAMILY, Stock.S_QUANTITY, quantity());
    for (int i = 0; i < Stock.S_DIST.length; ++i) {
      put.add(Const.TEXT_FAMILY, Stock.S_DIST[i], dist());
    }
    put.add(Const.NUMERIC_FAMILY, Stock.S_YTD, _0B);
    put.add(Const.NUMERIC_FAMILY, Stock.S_ORDER_CNT, _0B);
    put.add(Const.NUMERIC_FAMILY, Stock.S_REMOTE_CNT, _0B);
    put.add(Const.TEXT_FAMILY, Stock.S_DATA, data());

    put(put, Stock.TABLE);

    ++iid;
    if (iid >= POP_TOTAL_ID) {
      if (wid > POP_W_TO) return 0;
      ++wid;
      iid = 0;
    }
    return 1;
  }

  private byte[] data() {
    if (iid % 10 != 0) return Utils.randomString(26, 50).getBytes();
    int p = Utils.random(26, 42);
    StringBuffer sb = new StringBuffer(50);
    sb.append(Utils.randomString(26, p));
    sb.append("ORIGINAL");
    sb.append(Utils.randomString(p, 42));
    return sb.toString().getBytes();
  }

  static final byte[] _0B = Bytes.toBytes(0L);

  private byte[] dist() {
    return Utils.randomLetterString(24, 24).getBytes();
  }

  private byte[] quantity() {
    return Bytes.toBytes((long) Utils.random(10, 100));
  }

}
