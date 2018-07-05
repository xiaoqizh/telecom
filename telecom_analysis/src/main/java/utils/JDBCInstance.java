package utils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 16:17 2018/7/1
 * @Description:
 */

public class JDBCInstance {
    private static Connection connection = null;
    private JDBCInstance(){}

    /**
     *  得到Connection
     */
    public static Connection getConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                connection = JDBCUtil.getConnection();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
