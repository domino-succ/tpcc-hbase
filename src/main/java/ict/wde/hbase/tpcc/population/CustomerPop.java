package ict.wde.hbase.tpcc.population;

import ict.wde.hbase.tpcc.Const;
import ict.wde.hbase.tpcc.Utils;
import ict.wde.hbase.tpcc.table.Customer;
import ict.wde.hbase.tpcc.table.District;
import ict.wde.hbase.tpcc.table.History;
import ict.wde.hbase.tpcc.table.Warehouse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class CustomerPop extends DataPopulation {

  private HTableInterface ctable;
  private HTableInterface cidxtable;
  private HTableInterface htable;

  public CustomerPop(Configuration conf) throws IOException {
    super();
    ctable = new HTable(conf, Customer.TABLE);
    ctable.setAutoFlush(false);
    cidxtable = new HTable(conf, Customer.TABLE_INDEX_LAST);
    cidxtable.setAutoFlush(false);
    htable = new HTable(conf, History.TABLE);
    htable.setAutoFlush(false);
  }

  private int wid = POP_W_FROM;
  private int did = 0;
  private int id = 0;

  static final int POP_TOTAL_ID = 3000;

  @Override
  public int popOneRow() throws IOException {
    if (wid > POP_W_TO && did >= DistrictPop.POP_TOTAL_ID && id >= POP_TOTAL_ID) {
      ctable.close();
      htable.close();
      cidxtable.close();
      return 0;
    }
    byte[] w_id = Warehouse.toRowkey(wid);
    byte[] d_id = District.toDid(did);
    byte[] c_id = Customer.toCid(id);
    byte[] row = Customer.toRowkey(w_id, d_id, c_id);
    byte[] first = first();
    byte[] last = last();
    byte[] time = since();

    writeCustomerTable(w_id, d_id, c_id, row, last, first, time);
    writeCustomerIndex(w_id, d_id, c_id, row, last, first);
    writeHistoryTable(w_id, d_id, c_id, time);

    ++id;
    if (id >= POP_TOTAL_ID) {
      ++did;
      if (did >= DistrictPop.POP_TOTAL_ID) {
        if (wid > POP_W_TO) return 0;
        ++wid;
        did = 0;
      }
      id = 0;
    }
    return 3;
  }

  private void writeHistoryTable(byte[] w_id, byte[] d_id, byte[] c_id,
      byte[] time) throws IOException {
    byte[] row = History.toRowkey(w_id, d_id, c_id, time);
    Put put = new Put(row);
    put.add(Const.TEXT_FAMILY, History.H_C_ID, c_id);
    put.add(Const.TEXT_FAMILY, History.H_C_D_ID, d_id);
    put.add(Const.TEXT_FAMILY, History.H_C_W_ID, w_id);
    put.add(Const.TEXT_FAMILY, History.H_D_ID, d_id);
    put.add(Const.TEXT_FAMILY, History.H_W_ID, w_id);
    put.add(Const.TEXT_FAMILY, History.H_DATA, hdata());
    put.add(Const.NUMERIC_FAMILY, History.H_DATE, time);
    put.add(Const.NUMERIC_FAMILY, History.H_AMOUNT, AMOUNT0);
    put(put, htable);
  }

  static final byte[] AMOUNT0 = Bytes.toBytes(1000L);

  private byte[] hdata() {
    return Utils.randomString(12, 24).getBytes();
  }

  private void writeCustomerIndex(byte[] w_id, byte[] d_id, byte[] c_id,
      byte[] row, byte[] last, byte[] first) throws IOException {
    byte[] indexRow = Customer.toLastIndexRowkey(w_id, d_id, last, first);
    Put put = new Put(indexRow);
    put.add(Const.TEXT_FAMILY, c_id, row);
    put(put, cidxtable);
  }

  private void writeCustomerTable(byte[] w_id, byte[] d_id, byte[] c_id,
      byte[] row, byte[] last, byte[] first, byte[] time) throws IOException {
    Put put = new Put(row);
    put.add(Const.ID_FAMILY, Customer.C_W_ID, w_id);
    put.add(Const.ID_FAMILY, Customer.C_D_ID, d_id);
    put.add(Const.ID_FAMILY, Customer.C_ID, c_id);
    put.add(Const.TEXT_FAMILY, Customer.C_LAST, last);
    put.add(Const.TEXT_FAMILY, Customer.C_MIDDLE, MIDDLE0);
    put.add(Const.TEXT_FAMILY, Customer.C_FIRST, first);
    put.add(Const.TEXT_FAMILY, Customer.C_STREET_1, street());
    put.add(Const.TEXT_FAMILY, Customer.C_STREET_2, street());
    put.add(Const.TEXT_FAMILY, Customer.C_CITY, city());
    put.add(Const.TEXT_FAMILY, Customer.C_STATE, state());
    put.add(Const.TEXT_FAMILY, Customer.C_ZIP, Utils.randomZip());
    put.add(Const.TEXT_FAMILY, Customer.C_PHONE, phone());
    put.add(Const.NUMERIC_FAMILY, Customer.C_SINCE, time);
    put.add(Const.TEXT_FAMILY, Customer.C_CREDIT, credit());
    put.add(Const.NUMERIC_FAMILY, Customer.C_CREDIT_LIM, CREDITLIM0);
    put.add(Const.NUMERIC_FAMILY, Customer.C_DISCOUNT, discount());
    put.add(Const.NUMERIC_FAMILY, Customer.C_BALANCE, BALANCE0);
    put.add(Const.NUMERIC_FAMILY, Customer.C_YTD_PAYMENT, YTD0);
    put.add(Const.NUMERIC_FAMILY, Customer.C_PAYMENT_CNT, PAY0);
    put.add(Const.NUMERIC_FAMILY, Customer.C_DELIVERY_CNT, DELIVERY0);
    put.add(Const.TEXT_FAMILY, Customer.C_DATA, data());

    put(put, ctable);
  }

  private byte[] data() {
    return Utils.randomString(300, 500).getBytes();
  }

  private static final byte[] DELIVERY0 = Bytes.toBytes(0L);

  private static final byte[] PAY0 = Bytes.toBytes(1L);

  private static final byte[] YTD0 = Bytes.toBytes(1000L);

  private static final byte[] BALANCE0 = Bytes.toBytes(-1000L);

  private byte[] discount() {
    return Bytes.toBytes((long) Utils.random(0, 5000));
  }

  private static final byte[] CREDITLIM0 = Bytes.toBytes(5000000L);

  private static final byte[] CREDIT_GC = "GC".getBytes();
  private static final byte[] CREDIT_BC = "BC".getBytes();

  private byte[] credit() {
    if (Utils.random(1, 10) == 1) return CREDIT_BC;
    return CREDIT_GC;
  }

  private byte[] since() {
    return Utils.currentTimestamp();
  }

  private byte[] phone() {
    return Utils.randomDigitString(16, 16).getBytes();
  }

  private byte[] city() {
    return Utils.randomString(10, 20).getBytes();
  }

  private byte[] state() {
    return Utils.randomLetterString(2, 2).getBytes();
  }

  private byte[] street() {
    return Utils.randomString(10, 20).getBytes();
  }

  private byte[] first() {
    return Utils.randomString(8, 16).getBytes();
  }

  private static final byte[] MIDDLE0 = "OE".getBytes();

  private byte[] last() {
    if (id < 1000) {
      return Utils.lastName(id).getBytes();
    }
    return Utils.lastName(Utils.NURand_C_LAST()).getBytes();
  }

}
