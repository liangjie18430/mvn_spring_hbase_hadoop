package com.lj.conn;

public class HBaseConnectionFactory implements IConnectionProvider{

	@Override
	public IDataBaseConnection produce() {
		// TODO Auto-generated method stub
		return new HBaseConnection();
	}
	

}
