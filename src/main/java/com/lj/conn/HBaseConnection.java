package com.lj.conn;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.lj.tools.ResourcesConfig;

/**
 * 该类主要提供到hbase的连接，最好使用spring中的连接池
 * 这个类主要用来测试
 * @author Administrator
 *
 */
public class HBaseConnection implements IDataBaseConnection{
	//private static String configName = "hbase-site.xml";
	//取得Hbase的连接
/*	public static Connection getHbaseConn() throws IOException{
		Connection conn = ConnectionFactory.createConnection(ResourcesConfig.getHbaseConfig(configName));
		return conn;
	}*/
	
	public Connection getConnection(){
		Connection conn = null;
		try {
			conn = ConnectionFactory.createConnection(ResourcesConfig.getHbaseConfig());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conn;
	}
}
