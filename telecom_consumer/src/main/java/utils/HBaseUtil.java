package utils;

import hbase.CalleeWriteObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 20:58 2018/6/29
 * @Description:
 */

public class HBaseUtil {
    /**
     * 判断表是否存在
     */
    public static boolean isExistTable(Configuration configuration, String tableName) throws Exception {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));
        //但是资源一定要关闭
        admin.close();
        connection.close();
        return tableExists;
    }

    /**
     * 初始化命名空间 都是API的操作
     */
    public static void initNamespace(Configuration configuration, String namespace)  throws Exception {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //namespace就是相当于mysql中的database
        NamespaceDescriptor nd = NamespaceDescriptor
                .create(namespace)
                .addConfiguration("CREATE_TIME", String.valueOf(System.currentTimeMillis()))
                .addConfiguration("AUTHOR", "ZhangEnqi")
                .build();
        admin.createNamespace(nd);
        admin.close();
        connection.close();
    }

    /**
     * 创建表
     */
    public static void createTable(Configuration configuration, String tableName,int regionCount,String...columnFamily) throws Exception {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        if (isExistTable(configuration,tableName)) {
            return;
        }
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
//        htd.addCoprocessor()
        for (String cFamily : columnFamily) {
            //添加列蔟
            htd.addFamily(new HColumnDescriptor(cFamily));
        }
        //创建表同时也加上协处理器
//        htd.addCoprocessor("hbase.CalleeWriteObserver");

        //创建分区 防止以后数据倾斜
        admin.createTable(htd,genSplitKeys(regionCount));
        //
        admin.close();
        connection.close();

    }

    /**
     * 这是在创建表的时候就要把表进行分区region 就是通过下面的分区键进行区分的
     */
    private static byte[][] genSplitKeys(int regionCount) {
        //定义存放分区键的数组
        String[] keys = new String[regionCount];
        //DecimalFormat把整形格式化成字符串形式
        DecimalFormat df = new DecimalFormat("00");
        for (int i = 0; i < regionCount; i++) {
            keys[i] = df.format(i) + "|";
        }
        byte[][] splitKeys = new byte[regionCount][];
        //HBase的位比较器
        TreeSet<byte[]> treeSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        //把所有的bytes加进去
        for (int i = 0; i < regionCount; i++) {
            treeSet.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> iterator = treeSet.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            //把排序好的bytes 放回分区键中
            splitKeys[index++] = iterator.next();
        }
        for (byte[] splitKey : splitKeys) {
            System.out.println(Arrays.toString(splitKey));
        }
        /*
        ASCII码 0的为48  | 符号为124
        [48, 48, 124]
        [48, 49, 124]
        [48, 50, 124]
        [48, 51, 124]
        [48, 52, 124]
        [48, 53, 124]
         */
        return splitKeys;
    }

    /**
     * 生成rowKey
     * regionCode_call1_buildTime_call2_flag_duration
     */
    public static String genRowKey(String regionCode, String call1, String buildTime, String call2,
                            String flag, String duration) {
        StringBuilder sb = new StringBuilder();
        sb.append(regionCode + "_")
                .append(call1 + "_")
                .append(buildTime + "_")
                .append(call2 + "_")
                .append(flag + "_")
                .append(duration);
        return sb.toString();

    }

    /**
     * 手机号 15771711308
     * 通话建立时间 2017-01-10 11:20:30 ->20170110112030
     * 生成regionCode
     */
    public static String genRegionCode(String call1, String buildTime, int regionCount) {
        //离散形式的产生分区号
        //取出手机号后四位
        String last4num = call1.substring(call1.length() - 4);
        //取出年月 201703
        String ym = buildTime
                .trim()
                .replaceAll(":", "")
                .replace("-", "")
                .replace(" ", "")
                .substring(0, 6);
        //纯粹是为了离散数据
        Integer discrete = Integer.valueOf(last4num) ^ Integer.valueOf(ym);
        //这里有个bug  Integer的hashCode 还是其本身
        int hashCode = discrete.hashCode();
        //生成分区号
        int regionCode = hashCode % regionCount;
        /*
        这个分区号 是与分区密切联系的
        需要知道的是 hbase是字典序排序的
         */
        DecimalFormat df = new DecimalFormat("00");
        return df.format(regionCode);
    }
/*    public static void main(String[] args) {
        genSplitKeys(6);

    }*/
}
