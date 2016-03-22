package com.lj.conn;

public class SqlServerConnectionFactory implements IConnectionProvider{

	@Override
	public IDataBaseConnection produce() {
		// TODO Auto-generated method stub
		return new SqlServerConnection();
	}
	

}
