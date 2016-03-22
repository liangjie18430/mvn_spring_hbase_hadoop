package com.lj.conn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;



import java.sql.PreparedStatement;
/**
 * 取得到mysql的连接，并测试，成功
 * @author Administrator
 *
 */
public class MysqlConnection implements IDataBaseConnection{
	public static final String url = "jdbc:mysql://172.16.135.160/DSDATA";  
    public static final String DRIVER = "com.mysql.jdbc.Driver";  
    public static final String USER = "root";  
    public static final String PWD = "123456"; 
	
    
    public  Connection getConnection(){
    	Connection con=null;
        try{
            Class.forName(DRIVER);
        }catch(ClassNotFoundException e){
            System.out.println("加载驱动错误信息：" + e.getMessage());
        }
        try{
            con=DriverManager.getConnection(url,USER,PWD);
        }catch(SQLException e){
            System.out.println("数据库连接错误信息：" + e.getMessage());
            e.printStackTrace();
        }
        return con;
		
    }
    
    
    
  
    public static void main(String[] args) throws ClassNotFoundException, SQLException{
    	MysqlConnection test = new MysqlConnection();
    	Connection conn = test.getConnection();
    	String sql = "select count(*) as cnt from 代码表_地区;";
    	PreparedStatement pst = conn.prepareStatement(sql);
    	ResultSet rs = pst.executeQuery();
    	while(rs.next()){
    		System.out.println("记录数为："+rs.getString("cnt"));
    	}
    }

}
