package com.lj.model;

import java.util.List;

public class CommonModel {
	private List<String> fields;

	public List<String> getFields() {
		return fields;
	}

	public void setFields(List<String> fields) {
		this.fields = fields;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		String tostring = "";
		
		//System.out.println(getFields().size());
		for(int i =0;i<getFields().size();i++){
			tostring=tostring+getFields().get(i).toString()+"  ";
		}
		return tostring;
	}

}
