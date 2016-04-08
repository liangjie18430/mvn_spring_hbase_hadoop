package com.lj.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import com.lj.conn.IConnectionProvider;
import com.lj.conn.IDataBaseConnection;
import com.lj.conn.MysqlConnectionFactory;
import com.lj.model.CommonModel;
import com.lj.model.SqlTableHeadModel;
import com.lj.tools.PrintTool;
import com.mysql.jdbc.Connection;

public class MySqlDao implements IDao{
	private Connection conn;
	private PreparedStatement pst;
	private ResultSet rs;
	private ResultSetMetaData rsmd;

	public List<CommonModel> excuteSqlQueryToCommonModel(String sql)
			throws ClassNotFoundException, SQLException {
		
		pst = conn.prepareStatement(sql);
		rs = pst.executeQuery();
		rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		// 用来保存所有的对象
		List<CommonModel> lismodel = null;
		CommonModel model = null;
		List<String> lisvalue = null;
		// 这个对象只能在外面创建，不然一直重复new，对象中会一直只有一个CommonModel
		lismodel = new ArrayList<>();
		//当表头大于0时，才有可能执行以下内容
		if (columnCount > 0) {
			//这里为添加表头数据
			model = new CommonModel();
			lisvalue = new ArrayList<String>();
			for (int i = 1; i <= columnCount; i++) {
				lisvalue.add(rsmd.getColumnName(i).trim());
			}
			// 将一行的数据放入到一个model中
			model.setFields(lisvalue);
			// 将每个对象加入到队列中
			lismodel.add(model);

			while (rs.next()) {
				// 有数据的时候才初始化对象

				model = new CommonModel();
				lisvalue = new ArrayList<String>();
				// 将每一行的属性值加入
				for (int i = 1; i <= columnCount; i++) {
					lisvalue.add(rs.getString(i) == null ? "" : rs.getString(i).trim());
				}
				// 将一行的数据放入到一个model中
				model.setFields(lisvalue);
				// 将每个对象加入到队列中
				lismodel.add(model);
			}
		}
		// 执行完后，将所有关闭
		
		return lismodel;
	}
	
	/**
	 * 根据相应的sql语句，获取结果的各个列信息
	 * @param sql 需要查询的语句
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public List<SqlTableHeadModel> getAllTableHeadMessage(String sql)
			throws ClassNotFoundException, SQLException {
		
		pst = conn.prepareStatement(sql);
		rs = pst.executeQuery();
		rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		// 用来保存所有的对象
		List<SqlTableHeadModel> lismodel = null;
		SqlTableHeadModel tableMessModel = null;
		// 这个对象只能在外面创建，不然一直重复new，对象中会一直只有一个CommonModel
		lismodel = new ArrayList<>();
		//当表头大于0时，才有可能执行以下内容
		if (columnCount > 0) {
			//这里为添加表头数据
			
			for (int i = 1; i <= columnCount; i++) {
				
				//创建一个CommonModel对象保存所有信息
				tableMessModel = new SqlTableHeadModel();
				//获取表名
				tableMessModel.setColumnName(rsmd.getColumnName(i).trim()+"");
				//获取类型名
				tableMessModel.setColumnClassName(rsmd.getColumnClassName(i).trim()+"");
				tableMessModel.setColumnTypeName(rsmd.getColumnTypeName(i).trim());
				//某列类型的精确度(类型的长度)
				tableMessModel.setPrecision(rsmd.getPrecision(i)+"");
				// 是否自动递增 
				tableMessModel.setIsAutoIncrement(rsmd.isAutoIncrement(i)+"");
				
				lismodel.add(tableMessModel);
			}
		}
		// 执行完后，将所有关闭
		
		return lismodel;
	}
	
	public void init(){
		IConnectionProvider provider = new MysqlConnectionFactory();
		IDataBaseConnection obj = provider.produce();
		conn = (Connection) obj.getConnection();
	}
	public void close() {
		try {
			if (rs != null) {
				rs.close();
			}
			if (pst != null) {
				pst.close();
			}
			if (conn != null) {
				conn.close();
			}
		} catch (Exception ex) {

		}
	}

	public static void main(String[] args) throws ClassNotFoundException,
			SQLException {
		MySqlDao test = new MySqlDao();
		String sql = "select * from 分析表_近三年税款征收数据表_湖北省省本级 limit 0,1;";
		/*List<CommonModel> lismodel = test
				.excuteSqlQuery("show tables;");*/
		/*List<CommonModel> lismodel = test
				.excuteSqlQueryToCommonModel("select * from 分析表_近三年税款征收数据表_湖北省省本级 limit 0,1;");*/
		List<SqlTableHeadModel> lismodel = test.getAllTableHeadMessage(sql);
		PrintTool.print(lismodel);
	}

}
