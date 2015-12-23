package com.lj.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * 运行成功
 * @author Administrator
 *
 */
public class HBase_test {
	public static void main(String[] args) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "B507Server1,b507Server2,b507Server3");
		/** 与hbase/conf/hbase-site.xml中hbase.master配置的值相同 */
		conf.set("hbase.master", "172.16.135.160:60000");
		/** 与hbase/conf/hbase-site.xml中hbase.zookeeper.quorum配置的值相同 */
		conf.set("hbase.zookeeper.quorum", "B507Server1,b507Server2,b507Server3");
		/** 与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同 */
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		// HBaseAdmin admin = new HBaseAdmin(conf);
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		HTableDescriptor tableDescriptor = admin
				.getTableDescriptor(org.apache.hadoop.hbase.util.Bytes
						.toBytes("blog"));
		byte[] name = tableDescriptor.getName();
		System.out.println(new String(name));
		HColumnDescriptor[] columnFamilies = tableDescriptor
				.getColumnFamilies();
		for (HColumnDescriptor d : columnFamilies) {
			System.out.println(d.getNameAsString());
		}
	}

}
