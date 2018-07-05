package converter;

import kv.base.BaseDimension;
import kv.key.ContactDimension;
import kv.key.DateDimension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.JDBCInstance;
import utils.JDBCUtil;
import utils.LRUCache;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 11:57 2018/7/2
 * @Description:
 */

public class DimensionConvertImpl implements DimensionConvert {

    private static final Logger LOGGER = LoggerFactory.getLogger(DimensionConvertImpl.class);

    //对象线程化  jdbc连接器
    private ThreadLocal<Connection> threadLocal = new ThreadLocal<>();
    //内存缓存
    private LRUCache lruCache = new LRUCache(3000);

    public DimensionConvertImpl() {
        //jvm关闭时释放资源
        Runtime.getRuntime().addShutdownHook(new Thread(() -> JDBCUtil.close(threadLocal.get(), null,null)));
    }

    @Override
    public int getDimensionId(BaseDimension dimension) {
        //根据传入的对象获取主键id 首先从缓存中获取
        //时间维度 date_dimension_year_month_day,10
        //联系人维度 contact_dimension_telephone,12
        String cacheKey = genCacheKey(dimension);
        //获取缓存ID
        if (lruCache.containsKey(cacheKey)) {
            return lruCache.get(cacheKey);
        }
        //那么接下来就需要查询数据库
        //查询日期维度 或者 联系人维度

        String[] sqls = null;

        if (dimension instanceof DateDimension) {
            sqls = genDateDimensionSql();
        } else if (dimension instanceof ContactDimension) {
            sqls = genContactDimensionSql();

        }else {
            throw new RuntimeException("无匹配信息");
        }
        //进行查询
        Connection connection = this.getConnection();

        threadLocal.set(connection);

        int id = -1;
        synchronized (this) {
            id = execSQL(connection, sqls, dimension);
        }
        //加入缓存
        lruCache.put(cacheKey, id);
        return id;
    }

    /**
     * 第一个为查询 第二为插入
     */
    private int execSQL(Connection connection, String[] sqls, BaseDimension dimension) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(sqls[0]);
            setArgs(ps, dimension);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            //释放资源
//            JDBCUtil.close(connection, ps, rs);

            //否则就是执行插入
            connection.prepareStatement(sqls[1]);
            setArgs(ps, dimension);
            //执行插入
            ps.executeUpdate();
            //释放资源
//            JDBCUtil.close(null, ps, null);
            //然后再次查询
            ps = connection.prepareStatement(sqls[0]);
            setArgs(ps, dimension);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    /**
     * 根据不同维度来设置参数
     */
    private void setArgs(PreparedStatement ps, BaseDimension dimension) {
        int i = 0;
        try {
            if (dimension instanceof DateDimension) {
                DateDimension d = (DateDimension) dimension;
                ps.setString(++i,d.getYear());
                ps.setString(++i,d.getMonth());
                ps.setString(++i,d.getDay());
            } else if (dimension instanceof ContactDimension) {
                ContactDimension c = (ContactDimension) dimension;
                ps.setString(++i, c.getTelephone());
                ps.setString(++i, c.getName());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     */
    private Connection getConnection() {
        Connection connection = null;
    //    synchronized (this) {
            try {
                connection = threadLocal.get();
                if (connection == null || connection.isClosed()) {
                    connection = JDBCInstance.getConnection();
                    threadLocal.set(connection);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
      //  }
        return connection;
    }
    /**
     *
     */
    private String[] genContactDimensionSql() {
        String query = "select `id` from `tb_contacts` where `telephone` = ?  and `name` = ? order by `id` ;";
        String insert = "insert into `tb_contacts`  (`telephone`,'name') " +
                " values(?,?); ";
        return new String[]{query, insert};
    }

    private String[] genDateDimensionSql() {
        String query = "select `id` from `tb_dimension_date` where `year` = ? " +
                "and `month` = ? and `day` = ?  order by `id';";
        String insert = "insert into `tb_dimension_date` (`year`,`month`,`day`) " +
                " values(?,?,?); ";

        return new String[]{query, insert};
    }

    /**
     * 根据维度获得缓存
     */
    private String genCacheKey(BaseDimension dimension) {
        StringBuilder sb = new StringBuilder();
        if (dimension instanceof DateDimension) {
            DateDimension dateDimension = (DateDimension) dimension;
            sb.append("date_dimension")
                    .append(dateDimension.getYear())
                    .append(dateDimension.getMonth())
                    .append(dateDimension.getDay());
        } else if (dimension instanceof ContactDimension) {
            ContactDimension contactDimension = (ContactDimension) dimension;
            sb.append("contact_dimension")
                    .append(contactDimension.getTelephone());
        }

        return sb.toString();
    }

}
