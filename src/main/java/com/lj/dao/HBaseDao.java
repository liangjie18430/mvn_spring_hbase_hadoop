package com.lj.dao;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.lj.conn.HBaseConnectionFactory;
import com.lj.conn.IConnectionProvider;
import com.lj.conn.IDataBaseConnection;

/**
 * hbase在修改列的时候，必须先disable table
 * 
 * @author Administrator
 * 
 */
public class HBaseDao implements IDao{

	// 这里最好和最新版本的hbase操作文档对齐
	private Connection conn;
	private Admin admin;

	/**
	 * 创建一张表，如果表已存在，则覆盖已经存在的表，默认命名空间为 default
	 * 
	 * @param tableDesc
	 * @throws IOException
	 */
	public void createOrOveriteTable(HTableDescriptor tableDesc)
			throws IOException {

		if (admin.tableExists(tableDesc.getTableName())) {
			// 先停止table，再删除table
			admin.disableTable(tableDesc.getTableName());
			// Delete a table (Need to be disabled first)
			admin.deleteTable(tableDesc.getTableName());
		}
		// 然后创建table
		admin.createTable(tableDesc);
	}

	@SuppressWarnings("deprecation")
	public void createOrOveriteTable(HTableDescriptor tableDesc,
			String namespace) throws IOException {
		// 先创建一个命名空间
		NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(
				namespace).build();
		admin.createNamespace(namespaceDescriptor);
		// 然后将table的tablename设置，这个方法过时了，可能还没更新到setTableName
		tableDesc.setName(TableName.valueOf(namespace + ":"
				+ tableDesc.getTableName()));
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
	 * 通过一个HTableDescriptor创建一个表 测试成功
	 * 
	 * @param tableDesc
	 * @throws IOException
	 */
	public void createTable(HTableDescriptor tableDesc) throws IOException {
		if (admin.tableExists(tableDesc.getTableName()) == true) {
			System.out.println("表与存在");
		} else {
			// 然后创建table
			admin.createTable(tableDesc);
		}

	}

	/**
	 * 通过一个HTableDescriptor创建一个表 测试成功
	 * 
	 * @param tableDesc
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public void createTable(HTableDescriptor tableDesc, String namespace)
			throws IOException {

		tableDesc.setName(TableName.valueOf(namespace + ":"
				+ tableDesc.getTableName()));
		if (admin.tableExists(tableDesc.getTableName()) == true) {
			System.out.println("表与存在");
		} else {
			// 然后创建table
			admin.createTable(tableDesc);
		}

	}

	/**
	 * 通过一个表名删除一个表 测试成功
	 * 
	 * @param tableDesc
	 * @throws IOException
	 */
	public void createTable(String tablename) throws IOException {

		if (admin.tableExists(TableName.valueOf(tablename)) == true) {
			System.out.println("表已经存在");
		} else {
			// 然后创建table
			admin.createTable(new HTableDescriptor(TableName.valueOf(tablename)));
		}

	}

	/**
	 * 测试成功
	 * 
	 * @param tableDesc
	 * @throws IOException
	 */
	public void deleteTable(HTableDescriptor tableDesc) throws IOException {

		if (admin.tableExists(tableDesc.getTableName())) {
			// 先停止table，再删除table
			admin.disableTable(tableDesc.getTableName());
			// Delete a table (Need to be disabled first)
			admin.deleteTable(tableDesc.getTableName());
		}

	}
	
	/**
	 * 测试成功
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public void deleteTable(TableName tableName) throws IOException {

		HTableDescriptor tableDesc = admin.getTableDescriptor(tableName);
		if (admin.tableExists(tableDesc.getTableName())) {
			// 先停止table，再删除table
			admin.disableTable(tableDesc.getTableName());
			// Delete a table (Need to be disabled first)
			admin.deleteTable(tableDesc.getTableName());
		}

	}
	
	/**
	 * 测试成功
	 * 
	 * @param tableDesc
	 * @throws IOException
	 */
	public void deleteTable(String  tableName) throws IOException {
		HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(tableName));

		if (admin.tableExists(tableDesc.getTableName())) {
			// 先停止table，再删除table
			admin.disableTable(tableDesc.getTableName());
			// Delete a table (Need to be disabled first)
			admin.deleteTable(tableDesc.getTableName());
		}

	}
	
	

	/**
	 * 测试成功
	 * 获取默认default下的表
	 * @return
	 * @throws IOException
	 */
	public TableName[] getAllTable() throws IOException {

		// admin.listTableNames();
		// admin.listTables("blog", false)
		TableName[] tbName = admin.listTableNames();
		return tbName;
	}
	
	
	/**
	 * 测试成功
	 * 获取默认default下的表
	 * @return
	 * @throws IOException
	 */
	public TableName[] getAllTable(String nameSpace) throws IOException {

		// admin.listTableNames();
		// admin.listTables("blog", false)
		 TableName[] tbName = admin.listTableNamesByNamespace(nameSpace);

		return tbName;
	}

	/**
	 * 获取一个表的描述信息，传入HTableDescriptor类型
	 * 
	 * @param tabledesc
	 * @throws IOException
	 */
	public void getTableDiscribe(HTableDescriptor tabledesc) throws IOException {

		HTableDescriptor test = admin.getTableDescriptor(tabledesc
				.getTableName());
		HColumnDescriptor[] columns = test.getColumnFamilies();
		System.out.println("表名：" + test.getTableName());
		System.out.println("列族：");
		for (HColumnDescriptor hc : columns) {
			System.out.print(" " + hc.getNameAsString());
		}

	}

	/**
	 * 获取一个表的描述信息，传入String类型 测试成功
	 * 
	 * @param tabledesc
	 * @throws IOException
	 */
	public void getTableDiscribe(String tablename) throws IOException {

		HTableDescriptor test = admin.getTableDescriptor(TableName
				.valueOf(tablename));

		HColumnDescriptor[] columns = test.getColumnFamilies();
		System.out.println("表名：" + test.getTableName());
		System.out.println("列族：");
		for (HColumnDescriptor hc : columns) {
			System.out.print(" " + hc.getNameAsString());
		}

	}
	
	public HTableDescriptor getTableDescriptor(String nameSpace,String tableName) throws TableNotFoundException, IOException{
		return admin.getTableDescriptor(TableName
				.valueOf(nameSpace+":"+tableName));
		
	}
	
	public HTableDescriptor getTableDescriptor(String tableName) throws TableNotFoundException, IOException{
		return admin.getTableDescriptor(TableName
				.valueOf(tableName));
		
	}
	
	
	public HTable getTable(String nameSpace,String tableName) throws TableNotFoundException, IOException{
		return (HTable)conn.getTable(TableName.valueOf(nameSpace+":"+tableName));
		
	}
	
	public HTable getTable(String tableName) throws TableNotFoundException, IOException{
		return (HTable)conn.getTable(TableName.valueOf(tableName));
		
	}

	/**
	 * 通过一个HTableDescriptor获取表队列，测试成功
	 * 
	 * @param tabledesc
	 * @return
	 * @throws IOException
	 */
	public HTableDescriptor[] getTablesByTableDesc(HTableDescriptor tabledesc)
			throws IOException {

		// admin.listTableNames();
		// admin.listTables("blog", false)
		return admin.listTables("blog", false);
	}

	/**
	 * 通过一个HTableDescriptor获取单个表，测试成功
	 * 
	 * @param tabledesc
	 * @return
	 * @throws IOException
	 */
	public HTableDescriptor getTableByTableDesc(HTableDescriptor tabledesc)
			throws IOException {

		// admin.listTableNames();
		// admin.listTables("blog", false)

		HTableDescriptor[] htas = admin.listTables(tabledesc.getNameAsString(),
				false);
		return htas[0];
	}

	public String[] getAllNamespace() throws IOException {

		NamespaceDescriptor[] ss = admin.listNamespaceDescriptors();
		String[] namespaces = new String[ss.length];
		for (int i = 0; i < ss.length; i++) {
			namespaces[i] = ss[i].getName();
		}

		return namespaces;
	}
	
	
	
	
	

	/**
	 * 创建一个表空间，表空间类似于mysql中的database
	 * 
	 * @param namespace
	 * @throws IOException
	 */
	public void createNameSpace(String namespace) throws IOException {

		boolean result = existNamespace(namespace);
		if (result == true) {
			System.out.println("要创建 的命名空间已经存在");
		} else {
			admin.createNamespace(NamespaceDescriptor.create(namespace).build());
		}


	}

	private boolean existNamespace(String namespace) throws IOException {
		boolean result = false;
		NamespaceDescriptor[] namespaces = admin.listNamespaceDescriptors();
		for (NamespaceDescriptor ns : namespaces) {
			if (ns.getName().equals(namespace)) {
				result = true;
			}
		}
		return result;
	}
	
	
	 /**
     * put a cell data into a row identified by rowKey,columnFamily,identifier 
     * @param HTable, create by : HTable table = new HTable(conf, "tablename")
     * @param rowKey
     * @param columnFamily
     * @param identifier
     * @param data
     * @throws Exception
     */
    public void putCell(HTable table, String rowKey, String columnFamily, String qualifier, String value) throws Exception{
        Put p1 = new Put(Bytes.toBytes(rowKey));
        /*Cell cell = new Ce
        p1.add(kv)*/
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        //下面这个方法已经过时
        //p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(p1);
        System.out.println("put '"+rowKey+"', '"+columnFamily+":"+qualifier+"', '"+value+"'");
        table.flushCommits();
    }

	public void deleteNamespace(String nameSpace) throws IOException {
		//只有空的namespace才能被删除，所以得先删除所有的table
		TableName[] tableNames = getAllTable(nameSpace);
		//这里获取的tableName都会带上命名空间
		//PrintTool.print(tableNames);
		for(TableName tableName:tableNames){
			deleteTable(tableName);
		}
		admin.deleteNamespace(nameSpace);
	}
	
	 /**
     * get a row identified by rowkey
     * @param HTable, create by : HTable table = new HTable(conf, "tablename")
     * @param rowKey
     * @throws Exception
     */
    public static Result getRow(HTable table, String rowKey) throws Exception {
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        System.out.println("Get: "+result);
        return result;
    }
    
    /**
     * delete a row identified by rowkey
     * @param HTable, create by : HTable table = new HTable(conf, "tablename")
     * @param rowKey
     * @throws Exception
     */
    public void deleteRow(HTable table, String rowKey) throws Exception {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        System.out.println("Delete row: "+rowKey);
    }
    
    /**
     * return all row from a table
     * @param HTable, create by : HTable table = new HTable(conf, "tablename")
     * @throws Exception
     */
    public ResultScanner scanAll(HTable table) throws Exception {
        Scan s =new Scan();
        ResultScanner rs = table.getScanner(s);
      
        for(Result result:rs){
        	List<Cell> cells = result.listCells();
        	for(Cell cell:cells){
        		//byte[] rb = cell.getValueArray();
        		 String row = new String(result.getRow(),"UTF-8");
                 String family = new String(CellUtil.cloneFamily(cell),"UTF-8");
                 String qualifier = new String(CellUtil.cloneQualifier(cell),"UTF-8");
                 String value = new String(CellUtil.cloneValue(cell),"UTF-8");
                 System.out.println("[row:"+row+"],[family:"+family+"],[qualifier:"+qualifier+"],[value:"+value+"]");
        	}
        }
        return rs;
    }
    
    
    /**
     * return a range of rows specified by startrow and endrow
     * @param HTable, create by : HTable table = new HTable(conf, "tablename")
     * @param startrow
     * @param endrow
     * @throws Exception
     */
    public ResultScanner scanRange(HTable table,String startrow,String endrow) throws Exception {
        Scan s =new Scan(Bytes.toBytes(startrow),Bytes.toBytes(endrow));
        ResultScanner rs = table.getScanner(s);
        return rs;
    }
    
    
    /**
     * return a range of rows filtered by specified condition
     * @param HTable, create by : HTable table = new HTable(conf, "tablename")
     * @param startrow
     * @param filter
     * @throws Exception
     */
    public static ResultScanner scanFilter(HTable table,String startrow, Filter filter) throws Exception {
        Scan s =new Scan(Bytes.toBytes(startrow),filter);
        ResultScanner rs = table.getScanner(s);
        return rs;
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
		/*
		 * HBaseDao test = new HBaseDao(); test.init(); Admin admin=
		 * test.getAdmin(); admin. test.close();
		 */
	}

}
