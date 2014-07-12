package ict.wde.hbase.tpcc.population;

import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.tpcc.domino.DominoDriver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

public class TableCreator {

  private HTableDescriptor descriptor;
  private final Set<String> families = new HashSet<String>();

  public TableCreator(String name) {
    descriptor = new HTableDescriptor(name);
  }

  public void addColumn(String row) {	
    String[] info = row.split("\t");
    if (info.length == 0) return;
    String[] familyQualifier = info[0].split("\\.");
    if (families.contains(familyQualifier[0])) return;
    families.add(familyQualifier[0]);  //late-add
    HColumnDescriptor family = new HColumnDescriptor(familyQualifier[0]);
    descriptor.addFamily(family);
  }

  public void createIfNotExists(HBaseConnection conn) throws IOException {
    if (conn.tableExists(descriptor.getName())) return;
    conn.createTable(descriptor);
  }

  public void dropIfExists(HBaseConnection conn) throws IOException {
    if (!conn.tableExists(descriptor.getName())) return;
    conn.dropTable(descriptor.getName());
  }

  public static void main(String[] args) throws Exception {
    String tableConf = args[0];
    String zkAddr = args[1];
    boolean forceNew = args.length > 2 ? Boolean.parseBoolean(args[2]) : false;
    HBaseConnection conn = new DominoDriver().getConnection(zkAddr);
    BufferedReader reader = new BufferedReader(new FileReader(tableConf));
    String line;
    int status = 0;
    TableCreator creator = null;
    while ((line = reader.readLine()) != null) {
      line = line.trim();
      if ("".equals(line)) {
        status = 0;
        if (creator != null) {
          if (forceNew) creator.dropIfExists(conn);
          creator.createIfNotExists(conn);
          creator = null;
        }
        continue;
      }
      switch (status) {
      case 0: {
        creator = new TableCreator(line);
        status = 1;
        break;
      }
      case 1: {
        creator.addColumn(line);
        break;
      }
      }
    }
    if (creator != null) {
      creator.createIfNotExists(conn);
    }
    conn.close();
    reader.close();
  }
}
