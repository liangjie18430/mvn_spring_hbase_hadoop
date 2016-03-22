package com.lj.conn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SqlServerConnection implements IDataBaseConnection{
	//private static final String DRIVER="com.mysql.jdbc.Driver";
    private static final String DRIVER="com.microsoft.sqlserver.jdbc.SQLServerDriver";
    //private static final String URI="jdbc:mysql://localhost:3306/数据库名称";
    private static final String URI="jdbc:sqlserver://127.0.0.1:1433; DatabaseName=数据库名称";
    private static final String USER="sa";
    private static final String pwd="sasa";
	@Override
	public Connection getConnection(){
		// TODO Auto-generated method stub
		Connection con=null;
        try{
            Class.forName(DRIVER);
        }catch(ClassNotFoundException e){
            System.out.println("加载驱动错误信息：" + e.getMessage());
        }
        try{
            con=DriverManager.getConnection(URI,USER,pwd);
        }catch(SQLException e){
            System.out.println("数据库连接错误信息：" + e.getMessage());
            e.printStackTrace();
        }
        return con;
	}

}
