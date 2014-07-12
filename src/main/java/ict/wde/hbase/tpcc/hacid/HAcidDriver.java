package ict.wde.hbase.tpcc.hacid;

import java.io.IOException;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.driver.HBaseDriver;

public class HAcidDriver implements HBaseDriver {

  @Override
  public HBaseConnection getConnection(String address) throws IOException {
    return new HAcidConnection(address);
  }

}
