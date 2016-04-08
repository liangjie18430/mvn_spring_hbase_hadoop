package com.lj.dataresourcestohbase;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import com.lj.dao.HBaseDao;
import com.lj.dao.MySqlDao;
import com.lj.model.CommonModel;
import com.lj.model.SqlTableHeadModel;
import com.lj.tools.ChineseToEnglishTool;
import com.lj.tools.PrintTool;

public class MySqlToHBase {
	private final String nameSpace = "DSDATA";
	private HBaseDao hbaseDao = new HBaseDao();
	private MySqlDao mysqldao = new MySqlDao();

	private static Logger logger = Logger.getLogger(MySqlToHBase.class);
	

	public void tohbase() throws Exception {
		beforeMethod();
		
		// 获取所有DSDATA表中的所有表名
		List<CommonModel> tablenamelis = mysqldao
				.excuteSqlQueryToCommonModel("show tables;");
		List<SqlTableHeadModel> pertablemess;
		//PrintTool.print(tablenamelis);
		//通过一个表的列表，创建表
		//createTableList(tablenamelis);

		for (int i = 1; i < tablenamelis.size(); i++) {
			//
			String tableName = tablenamelis.get(i).toString().trim();
			// 获取每个表中的列信息
			PrintTool.print(tableName);
			//跳过代码表的数据迁移,因为主键不明确
			if(tableName.contains("代码表")){
				
			}
			else{
				putTableCell(tableName, hbaseDao, mysqldao);
			}

			// 然后是使用hbase的input方法添加的hbase中

		}
		afterMethod();

	}

	private void createTableList(List<CommonModel> tablenamelis) {
		// PrintTool.print(tablenamelis.get(0));
		// 跳过第一个数据，因为第一个数据是TALBE NAME这个名字
		// 循环创建表
		for (int i = 1; i < tablenamelis.size(); i++) {

			// 表创建完成
			createTableOne(tablenamelis.get(i).toString().trim());

		}
	}
	
	/**
	 * 因为需要映射中文标到英文表
	 * @param tableName
	 * @param hbaseDaoTest
	 * @param mysqlDaoTest
	 * @throws Exception
	 */
	public void putTableCell(String fromtableName,HBaseDao hbaseDaoTest,MySqlDao mysqlDaoTest) throws Exception{
		String nameSpace = "DSDATA";
		String columnFamily = "a";
		
		
		String sql = "select * from " + fromtableName;
		
		//获取中文表对应的英文标
		String totableName = ChineseToEnglishTool.getPinYinHeadChar(fromtableName);
		//HTableDescriptor htabledesc = hbaseDaoTest.getTableDescriptor(nameSpace, tableName);
		Table htable = hbaseDaoTest.getTable(nameSpace, totableName);
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

	/**
	 * 由于hbase中所有信息都是由byte[]数组组成，所以理论上迁移数据进入hbase时，是不需要进行相对应的类型转换
	 * 
	 * @param pertablemess
	 */
	private void createTableOne(String tableName) {

		// 第一步，将所有的中文表名换成英文表名
		// 注意添加上trim，不然可能会有空格，但是输出看不到
		String tableNamePy = ChineseToEnglishTool.getPinYinHeadChar(tableName)
				.trim();
		// 找出以数字开头的表，不能以数字开头

		/*
		 * if(tableNamePy.matches("[0-9]*")){ //将数字处理 tableNamePy.l }
		 */
		/*
		 * if(tableNamePy.contains("2012")){ tableNamePy =
		 * tableNamePy.substring(5).trim(); } PrintTool.print(tableNamePy);
		 */

		HTableDescriptor tableDesc = new HTableDescriptor(
				TableName.valueOf(tableNamePy));
		// 由于table中的schema是列族，不再说列，所以直接添加一个列族即可，
		// 根据hbase列族设计原则，列族的名字必须尽可能的短
		// 所以我们现在添加一个字母为a的列族即可
		HColumnDescriptor columnDesc = new HColumnDescriptor("a");
		tableDesc.addFamily(columnDesc);
		// 由于新建表是有namespace的，所以我们这里将nameSpace创建
		try {
			hbaseDao.createTable(tableDesc, "DSDATA");
			// 然后打印namespace下的所有表
			// PrintTool.print(hbaseDao.getAllTable("DSDATA"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 由于hbase中所有信息都是由byte[]数组组成，所以理论上迁移数据进入hbase时，是不需要进行相对应的类型转换
	 * 
	 * @param pertablemess
	 */
	public void generateHBaseSql(String tableName,
			List<SqlTableHeadModel> pertablemess) {

		// 第一步，将所有的中文表名换成英文表名

		String tableNamePy = ChineseToEnglishTool.getPinYinHeadChar(tableName);
		// PrintTool.print(tableNamePy);
		try {
			PrintTool.print(hbaseDao.getAllTable("DSDATA"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		 * HTableDescriptor tableDesc = new
		 * HTableDescriptor(TableName.valueOf(tableNamePy));
		 * //由于table中的schema是列族，不再说列，所以直接添加一个列族即可， //根据hbase列族设计原则，列族的名字必须尽可能的短
		 * //所以我们现在添加一个字母为a的列族即可 HColumnDescriptor columnDesc = new
		 * HColumnDescriptor("a"); tableDesc.addFamily(columnDesc);
		 * //由于新建表是有namespace的，所以我们这里将nameSpace创建 try {
		 * hbaseDao.createTable(tableDesc, "DSDATA"); //然后打印namespace下的所有表
		 * PrintTool.print(hbaseDao.getAllTable("DSDATA")); } catch (IOException
		 * e) { // TODO Auto-generated catch block e.printStackTrace(); }
		 */

	}
	
	
	
	private void beforeMethod() throws IOException{
		// 第一步：获取要转移数据库中的表名
				hbaseDao.init();
				mysqldao.init();
	}
	
	private void afterMethod() throws IOException{
		hbaseDao.close();
		mysqldao.close();
	}

}
