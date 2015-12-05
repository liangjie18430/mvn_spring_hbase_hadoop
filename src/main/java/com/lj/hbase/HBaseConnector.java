package com.lj.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseConnector {
	private static final String QUORUM="";
	private static final String CLIENT_PORT="2181";
	private HBaseAdmin admin;
	private Configuration conf;
	
	
	public HBaseAdmin getHBaseAdmin() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		getConfiguration();
		admin = new HBaseAdmin(conf);
		return admin;
	}
	
	
	public Configuration getConfiguration(){
		if(conf==null){
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", QUORUM);
			conf.set("hbase.zookeeper.property.clientPort", CLIENT_PORT);
		}
		return conf;
	}

}
