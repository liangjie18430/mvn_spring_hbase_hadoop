package com.lj.HadoopTest;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;


public class HadoopTest {
	@Test
	public  void test() throws IOException{
		String uri="hdfs:172.16.135.160";
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), config);
		//列出hdfs上
	}

}
