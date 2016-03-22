package com.lj.conn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleConnection implements IDataBaseConnection{
	//private static final String DRIVER="com.mysql.jdbc.Driver";
    private static final String DRIVER="oracle.jdbc.driver.OracleDriver";
    //private static final String URI="jdbc:oracle:thin:@localhost:1521:**";
    private static final String URI="jdbc.odbc:testConnectOracle";
    private static final String USER="system";
    private static final String pwd="**";
	@Override
	public Object getConnection(){
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
