package com.lj.mvn_ssh.service;

import java.sql.*;
public class JavaMysql
{
    public static void main(String[] args) throws Exception
    {
        String driver="com.mysql.jdbc.Driver";
        String url="jdbc:mysql://localhost:3306/test";
        Class.forName(driver);
        Connection connecter=DriverManager.getConnection(url,"root","123456");
            if(!connecter.isClosed()) System.out.println("success in getConnetion");
        Statement statement=connecter.createStatement();
        ResultSet rs=statement.executeQuery("select * from student");
        System.out.println("编号"+"\t"+"名字"+"\t"+"Age");
        String id=null,name=null,age=null;
        while(rs.next())
        {
            id=rs.getString("id");
            name=rs.getString("name");
            age=rs.getString("age");
   
            System.out.println(id+"\t"+name+"\t"+"\t"+age);
        }
    }

}