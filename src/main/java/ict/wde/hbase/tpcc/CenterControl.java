package ict.wde.hbase.tpcc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface CenterControl extends VersionedProtocol {

  public void reportMessage(String message);
  
  public void reportSummary(String summary);

}
