package ict.wde.hbase.tpcc.population;

import ict.wde.hbase.tpcc.Const;
import ict.wde.hbase.tpcc.Utils;
import ict.wde.hbase.tpcc.table.Item;
import ict.wde.hbase.tpcc.table.Warehouse;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class WarehousePop extends DataPopulation {

  static final byte[] YTD0 = Bytes.toBytes(30000000L);
  private int id = POP_W_FROM;
  private HTableInterface wtable;

  public WarehousePop(Configuration conf, int id) throws IOException {
    conf.set(HConstants.HBASE_CLIENT_INSTANCE_ID, id + "");
    wtable = new HTable(conf, Warehouse.TABLE);
    wtable.setAutoFlush(false);
  }

  private byte[] name() {
    return Utils.randomString(6, 10).getBytes();
  }

  private byte[] street1() {
    return Utils.randomString(10, 20).getBytes();
  }

  private byte[] street2() {
    return Utils.randomString(10, 20).getBytes();
  }

  private byte[] city() {
    return Utils.randomString(10, 20).getBytes();
  }

  private byte[] state() {
    return Utils.randomLetterString(2, 2).getBytes();
  }

  private byte[] tax() {
    return Bytes.toBytes((long) Utils.random(0, 2000));
  }

  @Override
  public int popOneRow() throws IOException {
    if (id > POP_W_TO) {
      wtable.close();
      return 0;
    }
    byte[] w_id = Warehouse.toRowkey(id);
    Put put = new Put(w_id);
    put.add(Const.ID_FAMILY, Warehouse.W_ID, w_id);
    put.add(Const.TEXT_FAMILY, Warehouse.W_NAME, name());
    put.add(Const.TEXT_FAMILY, Warehouse.W_STREET_1, street1());
    put.add(Const.TEXT_FAMILY, Warehouse.W_STREET_2, street2());
    put.add(Const.TEXT_FAMILY, Warehouse.W_CITY, city());
    put.add(Const.TEXT_FAMILY, Warehouse.W_STATE, state());
    put.add(Const.TEXT_FAMILY, Warehouse.W_ZIP, Utils.randomZip());
    put.add(Const.NUMERIC_FAMILY, Warehouse.W_TAX, tax());
    put.add(Const.NUMERIC_FAMILY, Warehouse.W_YTD, YTD0);
    put(put, wtable);
    ++id;
    return 1;
  }

}
