package ict.wde.hbase.tpcc.population;

import ict.wde.domino.common.DominoConst;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;

public class Main {

  static List<DataPopulation> tables = new ArrayList<DataPopulation>();

  public static void main(String[] args) {
    String zkAddr = args[0];
    String tabChars = args[1];
    Configuration config = new Configuration();
    config.set(DominoConst.ZK_PROP, zkAddr);
    try {
      initTablePopulation(tabChars, config);
    } catch (IOException e) {
      e.printStackTrace();
    }
    ExecutorService es = Executors.newCachedThreadPool();
    try {
      es.invokeAll(tables);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static void initTablePopulation(String chars, Configuration config) throws IOException {
    Set<Character> hash = new HashSet<Character>();
    for (char c : chars.toCharArray()) {
      int id = 0;
      if (hash.contains(c)) continue;
      switch (c) {
      case 'i':
        tables.add(new ItemPop(config, id ++));
        break;
      case 'w':
        tables.add(new WarehousePop(config, id ++));
        break;
      case 's':
        tables.add(new StockPop(config, id ++));
        break;
      case 'd':
        tables.add(new DistrictPop(config, id ++));
        break;
      case 'c':
        tables.add(new CustomerPop(config, id ++));
        break;
      case 'o':
        tables.add(new OrderPop(config, id ++));
        break;
      default:
        continue;
      }
      hash.add(c);
    }
  }
}
