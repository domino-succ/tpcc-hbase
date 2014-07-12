package ict.wde.hbase.driver;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public interface HBaseConnection extends Closeable {

  public void createTable(HTableDescriptor table) throws IOException;

  public boolean tableExists(byte[] table) throws IOException;

  public void dropTable(byte[] table) throws IOException;

  public void startTransaction() throws IOException;

  public void startReadOnlyTransaction() throws IOException;

  public void commit() throws IOException;

  public void rollback() throws IOException;

  public Result get(Get get, byte[] table) throws IOException;

  public void put(Put put, byte[] table) throws IOException;

  public void putStateless(Put put, byte[] table) throws IOException;

  public ResultScanner scan(Scan scan, byte[] table) throws IOException;

  public void delete(byte[] row, byte[] table) throws IOException;

}
