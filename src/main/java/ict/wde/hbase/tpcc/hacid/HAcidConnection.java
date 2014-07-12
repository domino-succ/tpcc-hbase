package ict.wde.hbase.tpcc.hacid;

import fi.aalto.hacid.HAcidClient;
import fi.aalto.hacid.HAcidGet;
import fi.aalto.hacid.HAcidPut;
import fi.aalto.hacid.HAcidTable;
import fi.aalto.hacid.HAcidTxn;
import fi.aalto.hacid.Schema;
import ict.wde.domino.common.Columns;
import ict.wde.hbase.driver.HBaseConnection;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HAcidConnection implements HBaseConnection {

  static final Logger LOG = LoggerFactory.getLogger(HAcidConnection.class);

  private Configuration conf = HBaseConfiguration.create();
  private HAcidClient client;
  private HBaseAdmin admin;

  public HAcidConnection(String zkAddr) throws IOException {
    conf.set("hbase.zookeeper.quorum", zkAddr);
    LOG.info("Got zk addr: {}", zkAddr);
    try {
      client = new HAcidClient(conf);
      admin = new HBaseAdmin(conf);
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    client.close();
    admin.close();
  }

  @Override
  public void createTable(HTableDescriptor table) throws IOException {

    // Source code from HAcid: Table creation in raw HAcid is really wierd.

    for (HColumnDescriptor family : table.getFamilies()) {
      family.setMaxVersions(Schema.MAX_VERSIONS_USERTABLE);
    }

    // Insert new column family 'HAcid'
    HColumnDescriptor hacidFamily = new HColumnDescriptor(Schema.FAMILY_HACID);
    hacidFamily.setMaxVersions(Schema.MAX_VERSIONS_USERTABLE);
    table.addFamily(hacidFamily);
    admin.createTable(table);
  }

  @Override
  public boolean tableExists(byte[] table) throws IOException {
    return admin.tableExists(table);
  }

  @Override
  public void dropTable(byte[] table) throws IOException {
    admin.disableTable(table);
    admin.deleteTable(table);
  }

  private HAcidTxn txn;

  @Override
  public void startTransaction() throws IOException {
    synchronized (this) {
      if (txn != null) return;
      LOG.info("Starting trasaction...");
      txn = new HAcidTxn(client);
      LOG.info("Started trasaction...");
    }
  }

  @Override
  public void commit() throws IOException {
    synchronized (this) {
      if (txn == null) return;
      if (!txn.commit()) {
        throw new IOException("Commit failed.");
      }
    }
  }

  @Override
  public void rollback() throws IOException {
    // do nothing..?
  }

  @Override
  public Result get(Get get, byte[] table) throws IOException {
    startTransaction();
    HAcidTable htable = new HAcidTable(conf, table);
    // try {
    HAcidGet hget = new HAcidGet(htable, get.getRow());
    if (get.hasFamilies()) {
      boolean getAll = false;
      for (byte[] family : get.familySet()) {
        if (get.getFamilyMap().get(family).size() == 0) {
          getAll = true;
          break;
        }
      }
      if (!getAll) {
        for (byte[] family : get.familySet()) {
          for (byte[] qualifier : get.getFamilyMap().get(family)) {
            hget.addColumn(family, qualifier);
          }
        }
      }
    }
    return txn.get(hget);
    // }
    // finally {
    // htable.close();
    // }
  }

  @Override
  public void put(Put put, byte[] table) throws IOException {
    startTransaction();
    HAcidTable htable = new HAcidTable(conf, table);
    // try {
    HAcidPut hput = new HAcidPut(htable, put.getRow());
    for (byte[] family : put.getFamilyMap().keySet()) {
      for (KeyValue kv : put.getFamilyMap().get(family)) {
        hput.add(family, kv.getQualifier(), kv.getValue());
      }
    }
    txn.put(hput);
    // }
    // finally {
    // htable.close();
    // }
  }

  @Override
  public void putStateless(Put put, byte[] table) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultScanner scan(Scan scan, byte[] table) throws IOException {
    HTable htable = new HTable(conf, table);
    try {
      return htable.getScanner(scan);
    }
    finally {
      htable.close();
    }
  }

  @Override
  public void delete(byte[] row, byte[] table) throws IOException {
    throw new UnsupportedOperationException();
  }

  public static void loadData(Put put, HTableInterface hti) throws IOException {
    hti.put(clonePut(put));
  }

  private static Put clonePut(Put put) {
    Put ret = new Put(put.getRow());
    Map<byte[], List<KeyValue>> families = put.getFamilyMap();
    Columns cols = new Columns(null);
    for (byte[] family : families.keySet()) {
      List<KeyValue> columns = families.get(family);
      Iterator<KeyValue> it = columns.iterator();
      while (it.hasNext()) {
        KeyValue kv = it.next();
        // byte[] column = DominoConst.getColumnKey(kv.getQualifier(), startId);
        byte[] qualifier = kv.getQualifier();
        ret.add(family, qualifier, Schema.TIMESTAMP_INITIAL_LONG, kv.getValue());
        cols.add(family, qualifier);
      }
    }
    Map<String, byte[]> attributes = put.getAttributesMap();
    for (String key : attributes.keySet()) {
      ret.setAttribute(key, attributes.get(key));
    }
    ret.add(Schema.FAMILY_HACID, Schema.QUALIFIER_USERTABLE_COMMITTED_AT,
        Schema.TIMESTAMP_INITIAL_LONG, Schema.TIMESTAMP_INITIAL);
    return ret;
  }

  @Override
  public void startReadOnlyTransaction() throws IOException {
    startTransaction();
  }

}
