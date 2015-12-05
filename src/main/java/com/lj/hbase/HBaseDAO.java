package com.lj.hbase;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 该代码包含了部分常用HBase接口。
 * @author Administrator
 *
 */
public class HBaseDAO {
    
    static Configuration conf = HBaseConfiguration.create();
    /**
     * create a table :table_name(columnFamily) 
     * @param tablename
     * @param columnFamily
     * @throws Exception
     */
    public static void createTable(String tablename, String columnFamily) throws Exception {
    	//官方中建议先创建连接然后再取得
     //   HBaseAdmin admin = new HBaseAdmin(conf);
        Admin admin = ConnectionFactory.createConnection(conf).getAdmin();
        if(admin.tableExists(TableName.valueOf(tablename))) {
            System.out.println("Table exists!");
            System.exit(0);
        }
        else {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            System.out.println("create table success!");
        }
        admin.close();
        
    }
    
    /**
     * delete table ,caution!!!!!! ,dangerous!!!!!!
     * @param tablename
     * @return
     * @throws IOException
     */
    public static boolean deleteTable(String tablename) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(tablename)) {
            try {
                admin.disableTable(tablename);
                admin.deleteTable(tablename);
            } catch (Exception e) {
                // TODO: handle exception
                e.printStackTrace();
                admin.close();
                return false;
            }
        }
        admin.close();
        return true;
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
    public static void putCell(HTable table, String rowKey, String columnFamily, String identifier, String data) throws Exception{
        Put p1 = new Put(Bytes.toBytes(rowKey));
        p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(identifier), Bytes.toBytes(data));
        table.put(p1);
        System.out.println("put '"+rowKey+"', '"+columnFamily+":"+identifier+"', '"+data+"'");
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
    public static void deleteRow(HTable table, String rowKey) throws Exception {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        System.out.println("Delete row: "+rowKey);
    }
    
    /**
     * return all row from a table
     * @param HTable, create by : HTable table = new HTable(conf, "tablename")
     * @throws Exception
     */
    public static ResultScanner scanAll(HTable table) throws Exception {
        Scan s =new Scan();
        ResultScanner rs = table.getScanner(s);
        return rs;
    }
    
    /**
     * return a range of rows specified by startrow and endrow
     * @param HTable, create by : HTable table = new HTable(conf, "tablename")
     * @param startrow
     * @param endrow
     * @throws Exception
     */
    public static ResultScanner scanRange(HTable table,String startrow,String endrow) throws Exception {
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
    
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

        HTable table = new HTable(conf, "apitable");
        
//      ResultScanner rs = HBaseDAO.scanRange(table, "2013-07-10*", "2013-07-11*");
//    	ResultScanner rs = HBaseDAO.scanRange(table, "100001", "100003");
    	ResultScanner rs = HBaseDAO.scanAll(table);

    	for(Result r:rs) {
    		System.out.println("Scan: "+r);
    	}
    	table.close();
    	

//      HBaseDAO.createTable("apitable", "testcf");
//      HBaseDAO.putRow("apitable", "100001", "testcf", "name", "liyang");
//      HBaseDAO.putRow("apitable", "100003", "testcf", "name", "leon");
//    	HBaseDAO.deleteRow("apitable", "100002");
//    	HBaseDAO.getRow("apitable", "100003");
//    	HBaseDAO.deleteTable("apitable");
        
    }
}