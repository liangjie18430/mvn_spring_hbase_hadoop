package com.lj.dataresourcestohbase;

import java.sql.SQLException;
import java.util.List;

import com.lj.dao.MySqlDao;
import com.lj.model.CommonModel;
import com.lj.model.SqlTableHeadModel;

public class MySqlToHBase {

	public void tohbase() throws ClassNotFoundException, SQLException {
		// 第一步：获取要转移数据库中的表名
		MySqlDao mysqldao = new MySqlDao();
		// 获取所有DSDATA表中的所有表名
		List<CommonModel> tablenamelis = mysqldao
				.excuteSqlQueryToCommonModel("show tables;");
		List<SqlTableHeadModel> pertablemess;
		// 跳过第一个数据，因为第一个数据是列名
		for (int i = 1; i < tablenamelis.size(); i++) {
			//
			String sql = "select * from " + tablenamelis.get(i) + " limit 0,1;";
			pertablemess = mysqldao.getAllTableHeadMessage(sql);
			// 第一个是表头信息
			generateHBaseSql(pertablemess);

		}

	}

	/**
	 * 由于hbase中所有信息都是由byte[]数组组成，所以理论上迁移数据进入hbase时，是不需要进行相对应的类型转换
	 * @param pertablemess
	 */
	public void generateHBaseSql(List<SqlTableHeadModel> pertablemess) {
		
		for (SqlTableHeadModel tablemess : pertablemess) {
			// tablemess

		}

	}


}
