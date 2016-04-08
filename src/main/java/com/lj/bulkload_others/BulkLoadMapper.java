package com.lj.bulkload_others;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 四个参数分别是输入的key value类类型
 * 和 keyvalue 的输出类型
 * @author Administrator
 *
 */
public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
	private String hbaseTable;
	private String dataSeperator;
	private String columnFamily1;
	private String columnFamily2;
	public void setup(Context context){
		Configuration configuration = context.getConfiguration();//获取作业参数
		hbaseTable = configuration.get("hbase.table.name");
		dataSeperator = configuration.get("dataseperator");
		columnFamily1 = configuration.get("COLUMN_FAMILY_1");
		columnFamily2 = configuration.get("COLUMN_FAMILY_2");
		
	}

	
	public void map(LongWritable key,Text value,Context context){
		try{
			//这里是将一行的value通过分隔符分割开来
			String[] values = value.toString().split(dataSeperator);
			ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
			Put put = new Put(Bytes.toBytes(values[0]));
			//对一个列族下添加俩个列，一个列为month，一个为day
			put.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("month"), Bytes.toBytes(values[1]));
			put.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("day"), Bytes.toBytes(values[2]));
			for(int i = 3;i<values.length;i++){
				put.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("hour:"+i), Bytes.toBytes(values[i]));
			}
			context.write(rowKey, put);
					
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
}
