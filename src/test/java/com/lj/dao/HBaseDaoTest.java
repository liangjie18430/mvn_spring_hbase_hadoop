package com.lj.dao;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.lj.model.CommonModel;
import com.lj.model.SqlTableHeadModel;
import com.lj.tools.PrintTool;

public class HBaseDaoTest {
	private HBaseDao hbaseDaoTest;
	private MySqlDao mysqlDaoTest;
	

	@Before
	public void beforeMethod() throws IOException {
		System.out.println("测试开始");

		hbaseDaoTest = new HBaseDao();
		hbaseDaoTest.init();
		mysqlDaoTest = new MySqlDao();
		mysqlDaoTest.init();
	}

	@Test
	public void testCreateTable() throws IOException {
		HTableDescriptor tabletest = initHTableCanTest("tese4");
		
		hbaseDaoTest.createTable(tabletest);
	}

	@Test
	public void testDeleteTable() throws IOException {
		HTableDescriptor tabletest = new HTableDescriptor(
				TableName.valueOf("test3"));
		hbaseDaoTest.deleteTable(tabletest);
	}

	@Test
	public void testgetAllNamespace() throws IOException{
		String[] namespaces = hbaseDaoTest.getAllNamespace();
		PrintTool.print(namespaces);
	}

	@Test
	public void testgetTableDiscribe() throws IOException {
		hbaseDaoTest.getTableDiscribe("test3");
	}
	@Test
	public void testCreateNamespace() throws IOException{
		hbaseDaoTest.createNameSpace("DSDATA");
	}
	
	@Test
	public void testGetAllTableByNamespace() throws IOException{
		PrintTool.print(hbaseDaoTest.getAllTable("DSDATA"));
	}

	@Test
	public void testGetAllTable() throws IOException {

		TableName[] htables = hbaseDaoTest.getAllTable();
		PrintTool.print(htables);
	}
	
	@Test
	public void testDeleteNamespace() throws IOException {

		hbaseDaoTest.deleteNamespace("DSDATA");
		//PrintTool.print(htables);
	}
	
	
	@Test
	public void testPutCell() throws Exception{
		String nameSpace = "DSDATA";
		String columnFamily = "a";
		
		String tableName = "USER";
		String sql = "select * from " + tableName;
		
		//HTableDescriptor htabledesc = hbaseDaoTest.getTableDescriptor(nameSpace, tableName);
		Table htable = hbaseDaoTest.getTable(nameSpace, tableName);
		List<CommonModel> allDataLis = null;
		// 查询出所有数据
		allDataLis = mysqlDaoTest.excuteSqlQueryToCommonModel(sql);
		//因为查询出来的数据中包含了表头信息，所以不需要单独查出来
		//PrintTool.print(allDataLis);
		List<String> biaotou = allDataLis.get(0).getFields();
		//需要从1开始
		for(int i = 1;i<allDataLis.size();i++){
			/*CommonModel data:allDataLis*/
			//获取数据
			List<String> fields = allDataLis.get(i).getFields();
			//由于第一列需要为关键字，并且插入表时，需要将关键字直接指定，所以这里虚幻遍历的时候跳过第一个关键字
			for(int j=1;j<fields.size();j++){
				String value = fields.get(j);
				//关键字是第一个，ID号
				String rowKey = fields.get(0);
				String qualifier = biaotou.get(j);
				//System.out.print("a:"+biaotou.get(j)+":"+value+" -- ");
				hbaseDaoTest.putCell((HTable)htable, rowKey, columnFamily, qualifier, value);
			}
			System.out.println("");
		}
		
		
	}
	
	
	@Test
	public void testScannerAll() throws Exception{
		String nameSpace = "DSDATA";
		String tableName = "USER";
		HTable htable = (HTable) hbaseDaoTest.getAdmin().getConnection().getTable(TableName.valueOf(nameSpace+":"+tableName));
		hbaseDaoTest.scanAll(htable);
		
	}

	@After
	public void afterMethod() throws IOException {
		System.out.println("测试结束");
		hbaseDaoTest.close();
		mysqlDaoTest.close();

	}

	private HTableDescriptor initHTableCanTest(String tablename) {
		HTableDescriptor htabDesc = new HTableDescriptor(
				TableName.valueOf(tablename));
		HColumnDescriptor column1 = new HColumnDescriptor("测试中文列");
		htabDesc.addFamily(column1);
		return htabDesc;
	}
	
	
}
