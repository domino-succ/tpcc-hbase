package ict.wde.hbase.tpcc.domino;

import java.io.IOException;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.driver.HBaseDriver;

public class DominoDriver implements HBaseDriver {

  @Override
  public HBaseConnection getConnection(String address) throws IOException {
    return new DominoConnection(address);
  }

}
