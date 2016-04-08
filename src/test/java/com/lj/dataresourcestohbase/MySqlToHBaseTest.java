package com.lj.dataresourcestohbase;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.lj.dao.HBaseDao;
import com.lj.dao.MySqlDao;


public class MySqlToHBaseTest {
	
	@Test
	public void testToHBase() throws Exception{
		MySqlToHBase sqlToHBase = new MySqlToHBase();
		try {
			//速度太慢，最好使用连接池的方式，每次插入的时候又把连接断开了
			sqlToHBase.tohbase();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	
	
	
	

}
