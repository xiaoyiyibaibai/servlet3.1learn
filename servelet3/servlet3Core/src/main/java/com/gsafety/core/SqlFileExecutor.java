package com.gsafety.core;

/**
 * Hello world!
 *
 */

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
/**
 * SQL 脚本执行类
 * @author kong
 *
 */
public final class SqlFileExecutor {
	
    public static void main(String[] args) {
        try {
        	String sql2 =SqlFileExecutor.class.getResource("/").getPath();
        	System.out.println("sql2="+sql2);
            List<String> sqlList = loadSql(sql2+"sql文件.sql");
            System.out.println("size:" + sqlList.size());
            for (String sql : sqlList) {
            	 Connection conn = null;
                 // MySQL的JDBC URL编写方式：jdbc:mysql://主机名称：连接端口/数据库的名称?参数=值
                 // 避免中文乱码要指定useUnicode和characterEncoding
                 // 执行数据库操作之前要在数据库管理系统上创建一个数据库，名字自己定，
                 // 下面语句之前就要先创建javademo数据库
                 String url = "jdbc:mysql://192.168.247.141:3306/demo?"
                         + "user=root&password=root&useUnicode=true&characterEncoding=UTF8";
          
                 try {
                     // 之所以要使用下面这条语句，是因为要使用MySQL的驱动，所以我们要把它驱动起来，
                     // 可以通过Class.forName把它加载进去，也可以通过初始化来驱动起来，下面三种形式都可以
                     Class.forName("com.mysql.jdbc.Driver");// 动态加载mysql驱动
                     // or:
                     // com.mysql.jdbc.Driver driver = new com.mysql.jdbc.Driver();
                     // or：
                     // new com.mysql.jdbc.Driver();
          
                     System.out.println("成功加载MySQL驱动程序");
                     // 一个Connection代表一个数据库连接
                     conn = DriverManager.getConnection(url);
                     // Statement里面带有很多方法，比如executeUpdate可以实现插入，更新和删除等
                     Statement stmt = conn.createStatement();
                   /*  sql = "create table student(NO char(20),name varchar(20),primary key(NO))";*/
                     int result = stmt.executeUpdate(sql);// executeUpdate语句会返回一个受影响的行数，如果返回-1就没有成功
                     if (result != -1) {
                        /* System.out.println("创建数据表成功");
                         sql = "insert into student(NO,name) values('2012001','陶伟基')";
                         result = stmt.executeUpdate(sql);
                         sql = "insert into student(NO,name) values('2012002','周小俊')";
                         result = stmt.executeUpdate(sql);
                         sql = "select * from student";
                         ResultSet rs = stmt.executeQuery(sql);// executeQuery会返回结果的集合，否则返回空值
                         System.out.println("学号\t姓名");
                         while (rs.next()) {
                             System.out
                                     .println(rs.getString(1) + "\t" + rs.getString(2));// 入如果返回的是int类型可以用getInt()
                         }*/
                     }
                 } catch (SQLException e) {
                     System.out.println("MySQL操作错误");
                     e.printStackTrace();
                 } catch (Exception e) {
                     e.printStackTrace();
                 } finally {
                     conn.close();
                 }
          
            } 
            
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
     
     
    /**
     * 读取 SQL 文件，获取 SQL 语句 
     * @param sqlFile
     *            SQL 脚本文件
     * @return List<sql> 返回所有 SQL 语句的 List
     * @throws Exception
     */
    private static List<String> loadSql(String sqlFile) throws Exception {
        List<String> sqlList = new ArrayList<String>();
        try {
            InputStream sqlFileIn = new FileInputStream(sqlFile);
            StringBuffer sqlSb = new StringBuffer();
            byte[] buff = new byte[1024];
            int byteRead = 0;
            while ((byteRead = sqlFileIn.read(buff)) != -1) {
                sqlSb.append(new String(buff, 0, byteRead));
            }
 
            // Windows 下换行是 \r\n, Linux 下是 \n
            String[] sqlArr = sqlSb.toString()
                    .split("(;\\s*\\r\\n)|(;\\s*\\n)");
            for (int i = 0; i < sqlArr.length; i++) {
                String sql = sqlArr[i].replaceAll("--.*", "").trim();
                if (!sql.equals("")) {
                    sqlList.add(sql);
                }
            }
            return sqlList;
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }
 
    /**
     * 传入连接来执行 SQL 脚本文件，这样可与其外的数据库操作同处一个事物中
     * 
     * @param conn
     *            传入数据库连接
     * @param sqlFile 
     *            SQL 脚本文件    可选参数，为空字符串或为null时 默认路径为 src/test/resources/config/script.sql
     * @throws Exception
     */
    public static void execute(Connection conn,String sqlFile) throws Exception {
        Statement stmt = null;
        if(sqlFile==null||"".equals(sqlFile)){
            sqlFile="src/test/resources/config/script.sql";
        }
        List<String> sqlList = loadSql(sqlFile);
        stmt = conn.createStatement();
        for (String sql : sqlList) {
            stmt.addBatch(sql);
        }
        int[] rows = stmt.executeBatch();
        System.out.println("Row count:" + Arrays.toString(rows));
    }
}

