package com.lj.model;

/**
 * 主要用来保存表头的信息
 * @author Administrator
 *
 */
public class SqlTableHeadModel {
	private String columnName;
	private String columnTypeName;
	private String columnClassName;
	private String precision;
	private String isAutoIncrement;
	private String columnLabel;
	//指定列的表目录名称
	private String catalog;
	
	public String getCatalog() {
		return catalog;
	}
	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}
	public String getColumnTypeName() {
		return columnTypeName;
	}
	public void setColumnTypeName(String columnTypeName) {
		this.columnTypeName = columnTypeName;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getColumnClassName() {
		return columnClassName;
	}
	public void setColumnClassName(String columnClassName) {
		this.columnClassName = columnClassName;
	}
	public String getPrecision() {
		return precision;
	}
	public void setPrecision(String precision) {
		this.precision = precision;
	}
	public String getIsAutoIncrement() {
		return isAutoIncrement;
	}
	public void setIsAutoIncrement(String isAutoIncrement) {
		this.isAutoIncrement = isAutoIncrement;
	}
	public String getColumnLabel() {
		return columnLabel;
	}
	public void setColumnLabel(String columnLabel) {
		this.columnLabel = columnLabel;
	}
	@Override
	public String toString() {
		return "SqlTableHeadModel [columnName=" + columnName
				+ ", columnTypeName=" + columnTypeName + ", columnClassName="
				+ columnClassName + ", precision=" + precision
				+ ", isAutoIncrement=" + isAutoIncrement + ", columnLabel="
				+ columnLabel + ", catalog=" + catalog + "]";
	}
	
	
	
	

}
