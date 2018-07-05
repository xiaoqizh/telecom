import hbase.HBaseDAO;
import org.apache.hadoop.conf.Configuration;
import utils.HBaseUtil;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 18:49 2018/6/30
 * @Description:
 */

public class HBaseUtilTest {
    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseDAO.configuration;
//        configuration.set("hbase.zookeeper.quorum", "xiaoqizh");
//        configuration.set("hbase.zookeeper.property.clientPort", "2184");
        boolean user = HBaseUtil.isExistTable(configuration, "user");
        System.out.println(user);

    }
}

