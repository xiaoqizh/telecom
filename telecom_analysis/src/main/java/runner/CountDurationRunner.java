package runner;

import kv.key.CommonDimension;
import kv.value.CountDurationValue;
import mapper.CountDurationMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import outputformat.MysqlOutputFormat;
import reducer.CountDurationReducer;

import java.io.IOException;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 11:03 2018/7/2
 * @Description:
 */

public class CountDurationRunner implements Tool {

    private Configuration configuration = null;

    @Override
    public int run(String[] strings) throws Exception {
        //得到conf
        //实例化job
        Job job = Job.getInstance();
        job.setJarByClass(CountDurationRunner.class);
        //组装Mapper
        //组装Reducer
        initHbaseInputConfig(job);
        initReducerOutputConfig(job);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 这是reducer的封装
     */
    private void initReducerOutputConfig(Job job)  {
        job.setReducerClass(CountDurationReducer.class);
        job.setOutputKeyClass(CommonDimension.class);
        job.setOutputValueClass(CountDurationValue.class);
        job.setOutputFormatClass(MysqlOutputFormat.class);
    }

    /**
     *  这里mapper的封装
     */
    private void initHbaseInputConfig(Job job) {
        //        Scan scan = new Scan();
//        //todo  这个地方设置scan的表  可能用来进行多表扫描
//        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("ns:calllog"));
//
//        TableMapReduceUtil.initTableMapperJob();
        Connection connection = null;
        Admin admin = null;
        try {
            String tableName = "ns:calllog";
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                throw new RuntimeException("table not exists");
            }
            //这里可以进行设置了
            Scan scan = new Scan();
            TableMapReduceUtil.initTableMapperJob(tableName, scan,
                    CountDurationMapper.class,
                    CommonDimension.class,
                    Text.class,
                    job,
                    true);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if (admin != null) {
                    admin.close();
                }
                if (connection != null && !connection.isClosed()) {
                    connection.close();

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = HBaseConfiguration.create(configuration);
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    public static void main(String[] args) {
        try {
            int status = ToolRunner.run(new CountDurationRunner(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
