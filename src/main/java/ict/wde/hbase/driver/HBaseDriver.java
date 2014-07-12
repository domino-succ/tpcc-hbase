package ict.wde.hbase.driver;

import java.io.IOException;

public interface HBaseDriver {

  public HBaseConnection getConnection(String address) throws IOException;

}
