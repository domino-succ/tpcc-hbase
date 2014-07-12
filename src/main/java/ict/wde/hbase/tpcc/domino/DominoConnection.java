package ict.wde.hbase.tpcc.domino;

import ict.wde.domino.client.Domino;
import ict.wde.domino.client.Transaction;
import ict.wde.hbase.driver.HBaseConnection;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class DominoConnection implements HBaseConnection {

  private Domino domino;
  private Transaction trans = null;

  protected DominoConnection(String addr) throws IOException {
    domino = new Domino(addr);
  }

  @Override
  public void close() throws IOException {
    domino.close();
  }

  @Override
  public void createTable(HTableDescriptor table) throws IOException {
    domino.createTable(table);
  }

  @Override
  public boolean tableExists(byte[] table) throws IOException {
    return domino.tableExists(table);
  }

  @Override
  public void dropTable(byte[] table) throws IOException {
    domino.dropTable(table);
  }

  @Override
  public void startTransaction() throws IOException {
    synchronized (this) {
      if (trans == null) {
        trans = domino.startTransaction();
      }
    }
  }

  @Override
  public void commit() throws IOException {
    synchronized (this) {
      if (trans == null) {
        return;
      }
      trans.commit();
      trans = null;
    }
  }

  @Override
  public void rollback() throws IOException {
    synchronized (this) {
      if (trans == null) {
        return;
      }
      trans.rollback();
      trans = null;
    }
  }

  @Override
  public Result get(Get get, byte[] table) throws IOException {
    if (trans == null) startTransaction();
    synchronized (this) {
      return trans.get(get, table);
    }
  }

  @Override
  public void put(Put put, byte[] table) throws IOException {
    if (trans == null) startTransaction();
    synchronized (this) {
      trans.putStateful(put, table);
    }
  }

  @Override
  public ResultScanner scan(Scan scan, byte[] table) throws IOException {
    synchronized (this) {
      if (trans == null) startTransaction();
      return trans.scan(scan, table);
    }
  }

  @Override
  public void delete(byte[] row, byte[] table) throws IOException {
    if (trans == null) startTransaction();
    synchronized (this) {
      trans.delete(row, table);
    }
  }

  @Override
  public void putStateless(Put put, byte[] table) throws IOException {
    if (trans == null) startTransaction();
    synchronized (this) {
      trans.put(put, table);
    }
  }

  @Override
  public void startReadOnlyTransaction() throws IOException {
    synchronized (this) {
      if (trans == null) {
        trans = domino.startReadOnlyTransaction();
      }
    }
  }

}
