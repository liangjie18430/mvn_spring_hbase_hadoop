package com.lj.conn;

public class MysqlConnectionFactory implements IConnectionProvider{

	@Override
	public IDataBaseConnection produce() {
		// TODO Auto-generated method stub
		return new MysqlConnection();
	}
	

}
