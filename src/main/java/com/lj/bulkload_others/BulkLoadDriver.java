package com.lj.bulkload_others;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.lj.tools.PrintTool;

public class BulkLoadDriver extends Configured implements Tool{
	//这里应该是设置数据间的间隔符
	private static final String DATA_SEPERATOR="\\s+";
	private static final String TABLE_NAME = "temperature";//表名
	private static final String COLUMN_FAMILY_1 = "date";//列名1
	private static final String COLUMN_FAMILY_2 = "temPerHour";//列名2
	

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String outputPath = args[1];
		/**
		 * 设置作业参数
		 */
		Configuration configuration = getConf();
		configuration.set("data.seperator", DATA_SEPERATOR);
		configuration.set("hbase.table.name", TABLE_NAME);
		configuration.set("COLUMN_FAMILY_1", COLUMN_FAMILY_1);
		configuration.set("COLUMN_FAMILY_2", COLUMN_FAMILY_2);
		Job job = Job.getInstance(configuration,"Bulk Load::"+TABLE_NAME);
		
		return 0;
	}
	
	public static void main(String[] args){
		try{
			//这里的configure，可以使用封装好的代码中的configuration
			int response = ToolRunner.run(HBaseConfiguration.create(), new BulkLoadDriver(), args);
			
			if(response==0){
				PrintTool.print("Job is successfully completed");
			}else{
				PrintTool.print("Job failed");
			}
		}catch(Exception ex){
			
		}
	}

}
