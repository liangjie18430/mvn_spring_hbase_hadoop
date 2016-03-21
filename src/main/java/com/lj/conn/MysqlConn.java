package com.lj.conn;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;

import java.sql.PreparedStatement;
/**
 * 取得到mysql的连接，并测试，成功
 * @author Administrator
 *
 */
public class MysqlConn {
	public static final String url = "jdbc:mysql://172.16.135.160/DSDATA";  
    public static final String name = "com.mysql.jdbc.Driver";  
    public static final String user = "root";  
    public static final String password = "123456"; 
	
    
    public  static Connection getConnection() throws ClassNotFoundException, SQLException{
    	Class.forName(name);//指定连接类型
    	Connection conn = (Connection) DriverManager.getConnection(url, user, password);//获取连接
		return conn;
    }
    
    
    
  
    public static void main(String[] args) throws ClassNotFoundException, SQLException{
    	
    	Connection conn = MysqlConn.getConnection();
    	String sql = "select count(*) as cnt from 代码表_地区;";
    	PreparedStatement pst = conn.prepareStatement(sql);
    	ResultSet rs = pst.executeQuery();
    	while(rs.next()){
    		System.out.println("记录数为："+rs.getString("cnt"));
    	}
    }

}
