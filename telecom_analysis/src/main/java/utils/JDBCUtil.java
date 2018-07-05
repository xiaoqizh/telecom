package utils;

import java.sql.*;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 16:07 2018/7/1
 * @Description: 用于数据库连接
 */

public class JDBCUtil {
    private final static String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private final static String MYSQL_URL = "jdbc:mysql://47.95.5.140:3306/telecom?useUnicode=true&charsetEncoding=UTF-8";
//    private final static String MYSQL_URL = "jdbc:mysql://47.95.5.140:3306/telecom?useUnicode=true&charsetEncoding=UTF-8";
    private final static String MYSQL_USERNAME = "root";
    private final static String MYSQL_PASSWORD = "uAiqwVwjJ8-i";

    /**
     * 实例化JDBC连接
     */
    public static Connection getConnection() {
        try {
            Class.forName(MYSQL_DRIVER);
            return DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
        } catch ( Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 释放连接器
     */
    public static void close(Connection connection, Statement statement, ResultSet resultSet) {
        try {
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();

            }
            if (statement != null && !statement.isClosed()) {
                statement.close();

            }
            if (connection != null && !connection.isClosed()) {
                statement.close();

            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
