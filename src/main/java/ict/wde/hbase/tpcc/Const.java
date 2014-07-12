package ict.wde.hbase.tpcc;

public class Const {

  public static final int W;

  public static final int A_C_LAST = 255;
  public static final int A_C_ID = 1023;
  public static final int A_OL_I_ID = 8191;

  public static final int X_C_LAST = 0;
  public static final int X_C_ID = 0;
  public static final int X_OL_I_ID = 0;

  public static final int Y_C_LAST = 999;
  public static final int Y_C_ID = 2999;
  public static final int Y_OL_I_ID = 99999;

  public static final int C_C_LAST;
  public static final int C_C_ID;
  public static final int C_OL_I_ID;

  // Used for TpccMeasurement to create child processes
  public static final int C_C_LAST_RUN;
  public static final int C_C_ID_RUN;
  public static final int C_OL_I_ID_RUN;

  static {
    W = Integer.parseInt(System.getProperty("W", "4"));
    // default is c-load
    C_C_LAST = Integer.parseInt(System.getProperty("C_C_LAST", "65"));
    C_C_ID = Integer.parseInt(System.getProperty("C_C_ID", "0"));
    C_OL_I_ID = Integer.parseInt(System.getProperty("C_OL_I_ID", "0"));
    C_C_ID_RUN = Utils.random(0, A_C_ID);
    C_OL_I_ID_RUN = Utils.random(0, A_OL_I_ID);
    int c_last_run;
    int c_delta;
    do {
      c_last_run = Utils.random(0, A_C_LAST);
      c_delta = c_last_run > C_C_LAST ? c_last_run - C_C_LAST : C_C_LAST
          - c_last_run;
    }
    while (c_delta < 65 || c_delta > 119 || c_delta == 96 || c_delta == 112);
    C_C_LAST_RUN = c_last_run;
  }

  public static final byte[] ID_FAMILY = "ID".getBytes();

  public static final byte[] TEXT_FAMILY = "TEXT".getBytes();

  public static final byte[] NUMERIC_FAMILY = "NUMERIC".getBytes();

  public static final byte[] INDEX_ROW_QUALIFIER = "KEY".getBytes();

}
