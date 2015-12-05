package com.lj.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;


public class MyExample {
	private static MyExample instance= null;
	//这里使用单例模式创建对象
	private MyExample(){
		
	}
	public static MyExample getInstance(){
		return instance==null?new MyExample():instance;
	}
	
	
	
	
	//设置一个默认的表名
	private String TABLE_NAME="test";
	private String CF_DEFAULT="DEFAULT_COLUMN_FAMILY";

	
	
	public Configuration getConfiguration(){
		Configuration config = HBaseConfiguration.create();
		/** 与hbase/conf/hbase-site.xml中hbase.master配置的值相同 */
		//这里为手动添加属性，也可以直接添加配置文件，hadoop的配置基于xml
		config.set("hbase.master", "192.16.135.160:60000");
		/** 与hbase/conf/hbase-site.xml中hbase.zookeeper.quorum配置的值相同 */
		config.set("hbase.zookeeper.quorum", "B507Server1,b507Server2,b507Server3");
		/** 与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同 */
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum", "B507Server1,b507Server2,b507Server3");
		return config;
		
	}
	public void createTable(Configuration config) throws IOException{
		
		
		//通过配置取得连接
		Connection conn = ConnectionFactory.createConnection(config);
		//通过连接获取一个admin
		Admin admin = conn.getAdmin();
		
		//每一个都有一个Descriptor，我们现在通过tabledescriptor创建一个表
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
		table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.SNAPPY));
		System.out.print("Creating table. ");
		createOrOverwrite(admin, table);
		System.out.println(" Done.");
	}
	
	
	//然后写一个创建表或者覆盖表的方法
	public void createOrOverwrite(Admin admin,HTableDescriptor table) throws IOException{
		//如果已经存在，则删除已经存在的table
		if(admin.tableExists(table.getTableName())){
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
	}

}
