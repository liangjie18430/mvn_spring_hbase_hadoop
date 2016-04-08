package com.lj.bulkload_others;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

import com.lj.conn.HBaseConnectionFactory;
import com.lj.conn.IConnectionProvider;
import com.lj.conn.IDataBaseConnection;
import com.lj.dao.HBaseDao;

public class HFileLoader {
	public static void doBulkLoad(String pathToHFile,String tableName){
		try{
			  Configuration configuration = new Configuration();
	            HBaseConfiguration.addHbaseResources(configuration);
	            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(configuration);
			IConnectionProvider provider = new HBaseConnectionFactory();
			IDataBaseConnection proconn = provider.produce();
			Connection connection = (Connection) proconn.getConnection();
			Admin admin = connection.getAdmin();
			HTableDescriptor htabledesc = new HTableDescriptor(TableName.valueOf("test"));
			HBaseDao dao = new HBaseDao();
			//loadFfiles.doBulkLoad(new Path(pathToHFile), dao.getTableByTableDesc(htabledesc));//导入数据
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
	}
}
