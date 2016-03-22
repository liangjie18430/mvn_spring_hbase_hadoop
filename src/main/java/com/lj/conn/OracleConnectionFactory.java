package com.lj.conn;

public class OracleConnectionFactory implements IConnectionProvider{

	@Override
	public IDataBaseConnection produce() {
		// TODO Auto-generated method stub
		return new OracleConnection();
	}
	

}
