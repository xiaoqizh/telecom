package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 20:58 2018/6/29
 * @Description:
 */

public class HBaseDAO {

    private int regionCount;
    private String namespace;
    private String tableName;
    public static final Configuration configuration;
    private Table table;
    private Connection connection;
    /**
     * 下面这个list 是用来缓冲put行为
     */
    //private  List<Put> putCacheList = new ArrayList<>();
    private SimpleDateFormat sdfFrom = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdfTo = new SimpleDateFormat("yyyyMMddHHmmss");
    static {
        configuration = HBaseConfiguration.create();
    }

    public HBaseDAO() {
        /*
        hbase.calllog.regioncount=6
        hbase.calllog.namespace=ns
        hbase.calllog.tablename=ns:calllog
         */
        try {
            regionCount = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regioncount"));
            namespace = PropertiesUtil.getProperty("hbase.calllog.namespace");
            tableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
            connection = ConnectionFactory.createConnection(configuration);
            if (!HBaseUtil.isExistTable(configuration, tableName)) {
                HBaseUtil.initNamespace(configuration, namespace);
                //todo 这里列蔟有问题
                HBaseUtil.createTable(configuration, tableName, regionCount, "f1","f2");
            }
            table = connection.getTable(TableName.valueOf(tableName));
//            table.set
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 把原始数据放入到HBase中
     * 1代表是主dong
     * HBase rowKey :  01_15771711308_20181206030959_16669326501_1_0007
     * HBase 列 ：     call1 call2 build_time build_time_ts flag  duration
     */
    public void put(String ori) throws ParseException {
        try {
            String[] splitedOri = ori.split(",");
            /*
            17529353996,16669326501,2018-12-06 03:09:59,0007
             */
            String caller = splitedOri[0];
            String callee = splitedOri[1];
            String buildTime = splitedOri[2];
            String duration = splitedOri[3];

            String regionCode = HBaseUtil.genRegionCode(caller, buildTime, regionCount);
            Date oriDate = sdfFrom.parse(buildTime);
            String buildTimeCleared = sdfTo.format(oriDate);
            String buildTimeTs = String.valueOf(oriDate.getTime());
            //生成roeKey
            String flag = "1";
            String rowKey = HBaseUtil.genRowKey(regionCode, caller, buildTimeCleared, callee, flag, duration);
            //Hbase中插入数据
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("call1"),Bytes.toBytes(caller));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("call2"),Bytes.toBytes(callee));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("build_time"),Bytes.toBytes(buildTime));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("build_time_ts"),Bytes.toBytes(buildTimeTs));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("flag"),Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("duration"),Bytes.toBytes(duration));
            System.out.println(rowKey);
            //todo 这个地方应该是使用协处理器  但是我不知道为啥那么就一直出错
            String regionCode2 = HBaseUtil.genRegionCode(callee, buildTime, regionCount);
            String flag2 = "0";
            String rowKey2 = HBaseUtil.genRowKey(regionCode2, callee, buildTimeCleared, caller, flag2, duration);

            Put put2 = new Put(Bytes.toBytes(rowKey2));
            put2.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("call1"),Bytes.toBytes(callee));
            put2.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("call2"),Bytes.toBytes(caller));
            put2.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("build_time"),Bytes.toBytes(buildTime));
            put2.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("build_time_ts"),Bytes.toBytes(buildTimeTs));
            put2.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("flag"),Bytes.toBytes(flag2));
            put2.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("duration"),Bytes.toBytes(duration));
            //加入到HBase中
            /*
            这里put 一个 put的list 会更快 因为是有缓存的
            默认是来一个put 就put一次  所以这次应该有一个缓存
            但是前提是必须关闭自动提交

            putCacheList.add(put);
            putCacheList.add(put2);

            if (putCacheList.size() > 5) {
                table.put(putCacheList)
                table.flushCommits()
            }
             */

            /*如果出现了 outOfMemory：could not create native thread
              这就是在linux上的对程序的 线程数的限制！
             */

            table.put(put);
            table.put(put2);
        } catch (IOException e) {
            e.printStackTrace();
        }
//        finally {
//            try {
//                table.close();
//                connection.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
    }
}
