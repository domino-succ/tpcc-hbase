package ict.wde.hbase.tpcc.population;

import java.io.BufferedReader;
import java.io.FileReader;


import ict.wde.hbase.driver.HBaseConnection;
import ict.wde.hbase.tpcc.domino.DominoDriver;

public class TableDroper {

	public static void main(String[] args) throws Exception {
		if( args.length != 2) return;
		String tableConf = args[0];
	    String zkAddr = args[1];
	    
	    HBaseConnection conn = new DominoDriver().getConnection(zkAddr);
	    BufferedReader reader = new BufferedReader(new FileReader(tableConf));
	    
	    String line;
	    int status = 0;
	    while ((line = reader.readLine()) != null) {
	    	line = line.trim();
	    	if( status == 0 ){
	    		if( conn.tableExists(line.getBytes()) )
	    			conn.dropTable(line.getBytes());
	    		status = 1;
	    	}else{
	    		if( "".equals(line) ) status = 0;
	    	}
	    }
	    
	    reader.close();
	    conn.close();
	}

}
