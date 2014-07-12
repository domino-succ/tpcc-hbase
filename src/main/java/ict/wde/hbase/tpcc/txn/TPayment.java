package ict.wde.hbase.tpcc.txn;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.tpcc.Const;
import ict.wde.hbase.tpcc.Utils;
import ict.wde.hbase.tpcc.table.Customer;
import ict.wde.hbase.tpcc.table.District;
import ict.wde.hbase.tpcc.table.History;
import ict.wde.hbase.tpcc.table.Warehouse;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class TPayment extends TpccTransaction {

  private int d_id;
  private int c_d_id;
  private int c_w_id;
  private String c_last;
  private int c_id;
  private long h_amount;
  private long h_date;

  static final String LINE01 = String.format("%32sPayment\n", "");
  static final String LINE02 = "Date: %1$td-%1$tm-%1$tY %1$tH:%1$tM:%1$tS\n";
  static final String LINE03 = "\n";
  static final String LINE04 = String.format(
      "Warehouse: %%05d%25sDistrict: %%02d\n", "");
  static final String LINE05 = String.format("%%-20s%21s%%-20s\n", "");
  static final String LINE06 = LINE05;
  static final String LINE07 = "%-20s %2s %10s       %-20s %2s %10s\n";
  static final String LINE08 = "\n";
  static final String LINE09 = "Customer: %s  Cust-Warehouse: %05d Cust-District: %02d\n";
  static final String LINE10 = "Name:   %-16s %2s %-16s     Since:  %4$td-%4$tm-%4$tY\n";
  static final String LINE11 = String.format(
      "        %%-20s%21sCredit: %%2s\n", "");
  static final String LINE12 = String.format(
      "        %%-20s%21s%%%%Disc:  %%1.2f\n", "");
  static final String LINE13 = "        %-20s %2s %10s       Phone:  %s\n";
  static final String LINE14 = "\n";
  static final String LINE15 = "Amount Paid:          $%7.2f      New Cust-Balance: $%13.2f\n";
  static final String LINE16 = "Credit Limit:   $% 13.2f\n";
  static final String LINE17 = "\n";
  static final String LINE18 = "Cust-Data: %s\n";// 49
  static final String LINECD = "           %s\n";
  static final String LINEEND = "\n";
  static final int C_DATA_LEN = 49;

  public TPayment(int w_id, HBaseConnection connection) {
    super(w_id, connection);
  }

  @Override
  public void generateInputData() {
    int x = Utils.random(1, 100);
    int y = Utils.random(1, 100);
    d_id = Utils.randomDid();
    if (x <= 85 || Const.W == 1) {
      c_w_id = w_id();
      c_d_id = d_id;
    }
    else {
      c_w_id = Utils.randomWidExcept(w_id());
      c_d_id = Utils.randomDid();
    }
    if (y <= 60) {
      this.c_last = Utils.lastName(Utils.NURand_C_LAST());
    }
    else {
      c_last = null;
      c_id = Utils.NURand_C_ID();
    }
    h_amount = Utils.random(100, 500000);
    h_date = System.currentTimeMillis();
  }

  @Override
  public void execute(HBaseConnection conn, StringBuffer output)
      throws IOException {
    // Warehouse-read
    byte[] wkey = Warehouse.toRowkey(w_id());
    Get wget = new Get(wkey);
    wget.addColumn(Const.TEXT_FAMILY, Warehouse.W_NAME);
    wget.addColumn(Const.TEXT_FAMILY, Warehouse.W_STREET_1);
    wget.addColumn(Const.TEXT_FAMILY, Warehouse.W_STREET_2);
    wget.addColumn(Const.TEXT_FAMILY, Warehouse.W_CITY);
    wget.addColumn(Const.TEXT_FAMILY, Warehouse.W_STATE);
    wget.addColumn(Const.TEXT_FAMILY, Warehouse.W_ZIP);
    wget.addColumn(Const.NUMERIC_FAMILY, Warehouse.W_YTD);
    Result wres = conn.get(wget, Warehouse.TABLE);
    long w_ytd = Utils
        .b2n(wres.getValue(Const.NUMERIC_FAMILY, Warehouse.W_YTD));
    String w_name = new String(wres.getValue(Const.TEXT_FAMILY,
        Warehouse.W_NAME));
    String w_str1 = new String(wres.getValue(Const.TEXT_FAMILY,
        Warehouse.W_STREET_1));
    String w_str2 = new String(wres.getValue(Const.TEXT_FAMILY,
        Warehouse.W_STREET_2));
    String w_city = new String(wres.getValue(Const.TEXT_FAMILY,
        Warehouse.W_CITY));
    String w_state = new String(wres.getValue(Const.TEXT_FAMILY,
        Warehouse.W_STATE));
    String w_zip = new String(wres.getValue(Const.TEXT_FAMILY, Warehouse.W_ZIP));
    // Warehouse-write
    Put wput = new Put(wkey);
    w_ytd += h_amount;
    wput.add(Const.NUMERIC_FAMILY, Warehouse.W_YTD, Utils.n2b(w_ytd));
    conn.put(wput, Warehouse.TABLE);

    // District-read
    byte[] did = District.toDid(d_id);
    byte[] dkey = District.toRowkey(wkey, did);
    Get dget = new Get(dkey);
    dget.addColumn(Const.TEXT_FAMILY, District.D_NAME);
    dget.addColumn(Const.TEXT_FAMILY, District.D_STREET_1);
    dget.addColumn(Const.TEXT_FAMILY, District.D_STREET_2);
    dget.addColumn(Const.TEXT_FAMILY, District.D_CITY);
    dget.addColumn(Const.TEXT_FAMILY, District.D_STATE);
    dget.addColumn(Const.TEXT_FAMILY, District.D_ZIP);
    dget.addColumn(Const.NUMERIC_FAMILY, District.D_YTD);
    Result dres = conn.get(dget, District.TABLE);
    String d_name = new String(
        dres.getValue(Const.TEXT_FAMILY, District.D_NAME));
    String d_str1 = new String(dres.getValue(Const.TEXT_FAMILY,
        District.D_STREET_1));
    String d_str2 = new String(dres.getValue(Const.TEXT_FAMILY,
        District.D_STREET_2));
    String d_city = new String(
        dres.getValue(Const.TEXT_FAMILY, District.D_CITY));
    String d_state = new String(dres.getValue(Const.TEXT_FAMILY,
        District.D_STATE));
    String d_zip = new String(dres.getValue(Const.TEXT_FAMILY, District.D_ZIP));
    long d_ytd = Utils.b2n(dres.getValue(Const.NUMERIC_FAMILY, District.D_YTD));
    // District-write
    Put dput = new Put(dkey);
    d_ytd += h_amount;
    dput.add(Const.NUMERIC_FAMILY, District.D_YTD, Utils.n2b(d_ytd));
    conn.put(dput, District.TABLE);

    // Customer id-read
    byte[] cwid = Warehouse.toRowkey(c_w_id);
    byte[] cdid = District.toDid(c_d_id);
    byte[] cid;
    if (this.c_last != null) {
      byte[][] cirange = Customer.toLastIndexQuery(cwid, cdid,
          this.c_last.getBytes());
      Scan ciscan = new Scan(cirange[0], cirange[1]);
      ciscan.addFamily(Const.TEXT_FAMILY);
      ResultScanner cirs = conn.scan(ciscan, Customer.TABLE_INDEX_LAST);
      LinkedList<byte[]> ckeys = new LinkedList<byte[]>();
      for (Result r : cirs) {
        Map<byte[], byte[]> familyMap = r.getFamilyMap(Const.TEXT_FAMILY);
        for (byte[] key : familyMap.keySet()) {
          ckeys.add(familyMap.get(key));
        }
      }
      Iterator<byte[]> it = ckeys.iterator();
      for (int i = 0, j = ckeys.size() / 2; i < j; ++i) {
        it.next();
      }
      cid = Customer.toCid(it.next());
    }
    else {
      cid = Customer.toCid(c_id);
    }
    // Customer-read
    byte[] ckey = Customer.toRowkey(cwid, cdid, cid);
    Get cget = new Get(ckey);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_FIRST);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_MIDDLE);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_LAST);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_STREET_1);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_STREET_2);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_CITY);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_STATE);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_ZIP);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_PHONE);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_CREDIT);
    cget.addColumn(Const.TEXT_FAMILY, Customer.C_DATA);
    cget.addColumn(Const.NUMERIC_FAMILY, Customer.C_SINCE);
    cget.addColumn(Const.NUMERIC_FAMILY, Customer.C_CREDIT_LIM);
    cget.addColumn(Const.NUMERIC_FAMILY, Customer.C_DISCOUNT);
    cget.addColumn(Const.NUMERIC_FAMILY, Customer.C_BALANCE);
    cget.addColumn(Const.NUMERIC_FAMILY, Customer.C_YTD_PAYMENT);
    cget.addColumn(Const.NUMERIC_FAMILY, Customer.C_PAYMENT_CNT);
    Result cres = conn.get(cget, Customer.TABLE);
    String c_first = new String(cres.getValue(Const.TEXT_FAMILY,
        Customer.C_FIRST));
    String c_middle = new String(cres.getValue(Const.TEXT_FAMILY,
        Customer.C_MIDDLE));
    String c_last = new String(
        cres.getValue(Const.TEXT_FAMILY, Customer.C_LAST));
    String c_str1 = new String(cres.getValue(Const.TEXT_FAMILY,
        Customer.C_STREET_1));
    String c_str2 = new String(cres.getValue(Const.TEXT_FAMILY,
        Customer.C_STREET_2));
    String c_city = new String(
        cres.getValue(Const.TEXT_FAMILY, Customer.C_CITY));
    String c_state = new String(cres.getValue(Const.TEXT_FAMILY,
        Customer.C_STATE));
    String c_zip = new String(cres.getValue(Const.TEXT_FAMILY, Customer.C_ZIP));
    String c_phone = new String(cres.getValue(Const.TEXT_FAMILY,
        Customer.C_PHONE));
    String c_credit = new String(cres.getValue(Const.TEXT_FAMILY,
        Customer.C_CREDIT));
    String c_data = null;
    if ("BC".equals(c_credit)) {
      c_data = new String(cres.getValue(Const.TEXT_FAMILY, Customer.C_DATA));
    }
    long c_since = Utils.b2n(cres.getValue(Const.NUMERIC_FAMILY,
        Customer.C_SINCE));
    long c_credit_lim = Utils.b2n(cres.getValue(Const.NUMERIC_FAMILY,
        Customer.C_CREDIT_LIM));
    long c_discount = Utils.b2n(cres.getValue(Const.NUMERIC_FAMILY,
        Customer.C_DISCOUNT));
    long c_balance = Utils.b2n(cres.getValue(Const.NUMERIC_FAMILY,
        Customer.C_BALANCE));
    long c_ytd_payment = Utils.b2n(cres.getValue(Const.NUMERIC_FAMILY,
        Customer.C_YTD_PAYMENT));
    long c_payment_cnt = Utils.b2n(cres.getValue(Const.NUMERIC_FAMILY,
        Customer.C_PAYMENT_CNT));
    // Customer-write
    Put cput = new Put(ckey);
    c_balance -= h_amount;
    cput.add(Const.NUMERIC_FAMILY, Customer.C_BALANCE, Utils.n2b(c_balance));
    c_ytd_payment += h_amount;
    cput.add(Const.NUMERIC_FAMILY, Customer.C_YTD_PAYMENT,
        Utils.n2b(c_ytd_payment));
    ++c_payment_cnt;
    cput.add(Const.NUMERIC_FAMILY, Customer.C_PAYMENT_CNT,
        Utils.n2b(c_payment_cnt));
    if ("BC".equals(c_credit)) {
      c_data = String.format("%s%s%s%s%s%1.2f%s", new String(cid), new String(
          cdid), new String(cwid), new String(did), new String(wkey),
          amount(h_amount), c_data);
      if (c_data.length() > 500) {
        c_data = c_data.substring(0, 500);
      }
      cput.add(Const.TEXT_FAMILY, Customer.C_DATA, c_data.getBytes());
    }
    conn.put(cput, Customer.TABLE);

    // History-write
    byte[] time = Utils.n2b(h_date);
    byte[] hkey = History.toRowkey(cwid, cdid, cid, time);
    Put hput = new Put(hkey);
    String h_data = String.format("%s    %s", w_name, d_name);
    hput.add(Const.TEXT_FAMILY, History.H_C_D_ID, cdid);
    hput.add(Const.TEXT_FAMILY, History.H_C_W_ID, cwid);
    hput.add(Const.TEXT_FAMILY, History.H_C_ID, cid);
    hput.add(Const.TEXT_FAMILY, History.H_W_ID, wkey);
    hput.add(Const.TEXT_FAMILY, History.H_D_ID, did);
    hput.add(Const.TEXT_FAMILY, History.H_DATA, h_data.getBytes());
    hput.add(Const.NUMERIC_FAMILY, History.H_AMOUNT, Utils.n2b(h_amount));
    hput.add(Const.NUMERIC_FAMILY, History.H_DATE, time);
    conn.put(hput, History.TABLE);

    // Output
    output.append(LINE01);
    output.append(String.format(LINE02, h_date));
    output.append(LINE03);
    output.append(String.format(LINE04, w_id(), d_id));
    output.append(String.format(LINE05, w_str1, d_str1));
    output.append(String.format(LINE06, w_str2, d_str2));
    output.append(String.format(LINE07, w_city, w_state, w_zip, d_city,
        d_state, d_zip));
    output.append(LINE08);
    output.append(String.format(LINE09, new String(cid), c_w_id, c_d_id));
    output.append(String.format(LINE10, c_first, c_middle, c_last, c_since));
    output.append(String.format(LINE11, c_str1, c_credit));
    output.append(String.format(LINE12, c_str2, discount(c_discount)));
    output.append(String.format(LINE13, c_city, c_state, c_zip, c_phone));
    output.append(LINE14);
    output.append(String.format(LINE15, amount(h_amount), amount(c_balance)));
    output.append(String.format(LINE16, amount(c_credit_lim)));
    output.append(LINE17);
    if (c_data != null) {
      if (c_data.length() > 200) c_data = c_data.substring(0, 200);
      String[] c_data_split;
      int split_len = (int) Math.ceil(c_data.length() / C_DATA_LEN);
      if (split_len == 0) {
        c_data_split = new String[] { "" };
      }
      else {
        c_data_split = new String[(int) Math.ceil(c_data.length() / 49)];
      }
      for (int i = 0; i < split_len; ++i) {
        int l = i * C_DATA_LEN;
        int r = l + C_DATA_LEN;
        if (r > c_data.length()) r = c_data.length();
        c_data_split[i] = c_data.substring(l, r);
      }
      output.append(String.format(LINE18, c_data_split[0]));
      for (int i = 1; i < split_len; ++i) {
        output.append(String.format(LINECD, c_data_split[i]));
      }
    }
    output.append(LINEEND);
  }

  private static double discount(long disc) {
    return disc / 100.0;
  }

  private static double amount(long amount) {
    return amount / 100.0;
  }

  @Override
  public boolean shouldCommit() {
    return true;
  }

  @Override
  public void waitForKeying() {
    Utils.sleep(3000);
  }

  @Override
  public void waitForThinking() {
    Utils.sleep(Utils.thinkingTime(12));
  }

  @Override
  public String getReportMessage() {
    return Character.toString(TpccTransaction.PAYMENT);
  }

  @Override
  public char getType() {
    return TpccTransaction.PAYMENT;
  }

}
