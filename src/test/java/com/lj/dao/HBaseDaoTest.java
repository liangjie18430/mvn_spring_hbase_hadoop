package com.lj.dao;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.lj.tools.PrintTool;

public class HBaseDaoTest {
	private HBaseDao test;

	@Before
	public void beforeMethod() throws IOException {
		System.out.println("测试开始");

		test = new HBaseDao();
	}

	@Test
	public void testCreateTable() throws IOException {
		HTableDescriptor tabletest = initHTableCanTest("test3");
		test.createTable(tabletest);
	}

	@Test
	public void testDeleteTable() throws IOException {
		HTableDescriptor tabletest = new HTableDescriptor(
				TableName.valueOf("test3"));
		test.deleteTable(tabletest);
	}

	@Test
	public void testgetTableDiscribe() throws IOException {
		test.getTableDiscribe("test3");
	}

	@Test
	public void testGetAllTable() throws IOException {

		TableName[] htables = test.getAllTable();
		PrintTool.print(htables);
	}

	@After
	public void afterMethod() throws IOException {
		System.out.println("测试结束");

	}

	private HTableDescriptor initHTableCanTest(String tablename) {
		HTableDescriptor htabDesc = new HTableDescriptor(
				TableName.valueOf(tablename));
		HColumnDescriptor column1 = new HColumnDescriptor("测试中文列");
		htabDesc.addFamily(column1);
		return htabDesc;
	}
}
