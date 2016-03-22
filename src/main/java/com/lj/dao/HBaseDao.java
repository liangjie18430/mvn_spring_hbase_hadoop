package com.lj.dao;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import com.lj.conn.HBaseConnectionFactory;
import com.lj.conn.IConnectionProvider;
import com.lj.conn.IDataBaseConnection;

public class HBaseDao {

	// 这里最好和最新版本的hbase操作文档对齐
	private Connection conn;
	private Admin admin;

	public void createOrOveriteTable(HTableDescriptor tableDesc)
			throws IOException {

		init();
		if (admin.tableExists(tableDesc.getTableName())) {
			// 先停止table，再删除table
			admin.disableTable(tableDesc.getTableName());
			// Delete a table (Need to be disabled first)
			admin.deleteTable(tableDesc.getTableName());
		}
		// 然后创建table
		admin.createTable(tableDesc);
		close();
	}

	/**
	 * 通过一个HTableDescriptor删除一个表
	 * 测试成功
	 * @param tableDesc
	 * @throws IOException
	 */
	public void createTable(HTableDescriptor tableDesc) throws IOException {

		init();
		if (admin.tableExists(tableDesc.getTableName()) == true) {
			System.out.println("表与存在");
		} else {
			// 然后创建table
			admin.createTable(tableDesc);
		}
		close();
	}
	
	
	/**
	 * 通过一个表名删除一个表
	 * 测试成功
	 * @param tableDesc
	 * @throws IOException
	 */
	public void createTable(String tablename) throws IOException {

		init();
		if (admin.tableExists(TableName.valueOf(tablename)) == true) {
			System.out.println("表已经存在");
		} else {
			// 然后创建table
			admin.createTable(new HTableDescriptor(TableName.valueOf(tablename)));
		}
		close();
	}
	

	/**
	 * 测试成功
	 * @param tableDesc
	 * @throws IOException
	 */
	public void deleteTable(HTableDescriptor tableDesc) throws IOException {
		init();
		if (admin.tableExists(tableDesc.getTableName())) {
			// 先停止table，再删除table
			admin.disableTable(tableDesc.getTableName());
			// Delete a table (Need to be disabled first)
			admin.deleteTable(tableDesc.getTableName());
		}
		close();
	}

	/**
	 * 测试成功
	 * @return
	 * @throws IOException
	 */
	public TableName[] getAllTable() throws IOException {
		init();
		// admin.listTableNames();
		// admin.listTables("blog", false)
		TableName[] tbName = admin.listTableNames();
		close();
		return tbName;
	}
	
	/**
	 * 获取一个表的描述信息，传入HTableDescriptor类型
	 * @param tabledesc
	 * @throws IOException 
	 */
	public void getTableDiscribe(HTableDescriptor tabledesc) throws IOException{
		init();
		HTableDescriptor test = admin.getTableDescriptor(tabledesc.getTableName());
		 HColumnDescriptor[] columns = test.getColumnFamilies();
		 System.out.println("表名："+test.getTableName());
		 System.out.println("列族：");
		 for(HColumnDescriptor hc:columns){
			 System.out.print(" "+hc.getNameAsString());
		 }
		close();
	}
	
	
	/**
	 * 获取一个表的描述信息，传入String类型
	 * 测试成功
	 * @param tabledesc
	 * @throws IOException 
	 */
	public void getTableDiscribe(String tablename) throws IOException{
		init();
		HTableDescriptor test = admin.getTableDescriptor(TableName.valueOf(tablename));
		
		 HColumnDescriptor[] columns = test.getColumnFamilies();
		 System.out.println("表名："+test.getTableName());
		 System.out.println("列族：");
		 for(HColumnDescriptor hc:columns){
			 System.out.print(" "+hc.getNameAsString());
		 }
		close();
	}
	
	

	/**
	 * 通过一个HTableDescriptor获取表，测试成功
	 * @param tabledesc
	 * @return
	 * @throws IOException
	 */
	public HTableDescriptor[] getTableByTableDesc(HTableDescriptor tabledesc)
			throws IOException {
		init();
		// admin.listTableNames();
		// admin.listTables("blog", false)
		return admin.listTables("blog", false);
	}

	public void init() throws IOException {
		IConnectionProvider provider = new HBaseConnectionFactory();
		IDataBaseConnection obj = provider.produce();
		conn = (Connection) obj.getConnection();
		// 获取admin对象
		admin = conn.getAdmin();
	}

	public void close() throws IOException {
		if (admin != null) {
			admin.close();
			if (conn != null) {
				conn.close();
			}
		}
	}
	
	
	
	
	
	
	

	public Connection getConn() {
		return conn;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

	public Admin getAdmin() {
		return admin;
	}

	public void setAdmin(Admin admin) {
		this.admin = admin;
	}

	public static void main(String[] args) throws IOException {
		/*HBaseDao test = new HBaseDao();
		test.init();
		Admin admin= test.getAdmin();
		admin.
		test.close();*/
	}
	
	

}
