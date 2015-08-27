package ict.wde.hbase.tpcc.population;

import ict.wde.hbase.tpcc.Const;
import ict.wde.hbase.tpcc.Utils;
import ict.wde.hbase.tpcc.table.District;
import ict.wde.hbase.tpcc.table.Warehouse;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class DistrictPop extends DataPopulation {

  static final int POP_TOTAL_ID = 10;
  private int wid = POP_W_FROM;
  private int did = 0;
  private HTableInterface dtable;

  public DistrictPop(Configuration conf, int id) throws IOException {
    conf.set(HConstants.HBASE_CLIENT_INSTANCE_ID, id + "");
    dtable = new HTable(conf, District.TABLE);

  }

  public DistrictPop(Configuration conf, ThreadPoolExecutor pool) throws IOException {
    HConnection conn = HConnectionManager.createConnection(conf);
    dtable = new HTable(District.TABLE, conn, pool);
    dtable.setAutoFlush(false);
  }
  @Override
  public int popOneRow() throws IOException {
    if (wid > POP_W_TO) {
      dtable.close();
      return 0;
    }

    byte[] w_id = Warehouse.toRowkey(wid);
    byte[] d_id = District.toDid(did);
    Put put = new Put(District.toRowkey(w_id, d_id));
    put.add(Const.ID_FAMILY, District.D_W_ID, w_id);
    put.add(Const.ID_FAMILY, District.D_ID, d_id);
    put.add(Const.TEXT_FAMILY, District.D_NAME, name());
    put.add(Const.TEXT_FAMILY, District.D_STREET_1, street());
    put.add(Const.TEXT_FAMILY, District.D_STREET_2, street());
    put.add(Const.TEXT_FAMILY, District.D_CITY, city());
    put.add(Const.TEXT_FAMILY, District.D_STATE, state());
    put.add(Const.TEXT_FAMILY, District.D_ZIP, Utils.randomZip());
    put.add(Const.NUMERIC_FAMILY, District.D_TAX, tax());
    put.add(Const.NUMERIC_FAMILY, District.D_YTD, YTD0);
    put.add(Const.NUMERIC_FAMILY, District.D_NEXT_O_ID, NEXT_O_ID0);

    put(put, dtable);

    ++did;
    if (did >= POP_TOTAL_ID) {
      ++wid;
      did = 0;
    }
    return 1;
  }

  static final byte[] YTD0 = Bytes.toBytes(3000000L);
  static final byte[] NEXT_O_ID0 = Bytes.toBytes(3001L);

  private byte[] tax() {
    return Bytes.toBytes((long) Utils.random(0, 2000));
  }

  private byte[] state() {
    return Utils.randomLetterString(2, 2).getBytes();
  }

  private byte[] city() {
    return Utils.randomString(10, 20).getBytes();
  }

  private byte[] street() {
    return Utils.randomString(10, 20).getBytes();
  }

  private byte[] name() {
    return Utils.randomString(6, 10).getBytes();
  }

}
