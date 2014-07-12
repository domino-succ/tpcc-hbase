package ict.wde.hbase.driver;

public class HBaseDriverManager {

  public static HBaseDriver getDriver(String className)
      throws ClassNotFoundException, InstantiationException,
      IllegalAccessException {
    return (HBaseDriver) Class.forName(className).newInstance();
  }

}
