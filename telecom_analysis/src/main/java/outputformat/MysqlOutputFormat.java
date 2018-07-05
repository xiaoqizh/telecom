package outputformat;

import converter.DimensionConvert;
import converter.DimensionConvertImpl;
import kv.key.CommonDimension;
import kv.value.CountDurationValue;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.JDBCInstance;
import utils.JDBCUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 11:28 2018/7/2
 * @Description:
 */

public class MysqlOutputFormat extends OutputFormat<CommonDimension, CountDurationValue> {
    private OutputCommitter committer = null;


    @Override
    public RecordWriter<CommonDimension, CountDurationValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Connection conn = null;
        conn = JDBCInstance.getConnection();
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
        return new MysqlRecordWriter(conn);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        //检验
    }


    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        /*
        真的不知道这是干嘛用的
         */
        if (committer == null) {
            String name = context.getConfiguration().get(FileOutputFormat.OUTDIR);
            Path outputPath = name == null ? null : new Path(name);
            committer = new FileOutputCommitter(outputPath, context);
        }
        return committer;
    }

    /**
     * 自定义输出类
     */
    static class MysqlRecordWriter extends RecordWriter<CommonDimension, CountDurationValue> {

        private DimensionConvertImpl dc = new DimensionConvertImpl();
        private Connection connection;
        private PreparedStatement ps;
        private String insertSQL;
        private int count = 0;
        private final int BATCH_SIZE = 500;

        public MysqlRecordWriter(Connection conn) {
            this.connection = conn;
        }

        @Override
        public void write(CommonDimension key, CountDurationValue value) throws IOException, InterruptedException {
            //tb_call
            //id_date_contact, id_date_dimension, id_contact, call_sum, call_duration_sum
            int idDateDimension = dc.getDimensionId(key.getDateDimension());
            int idContact = dc.getDimensionId(key.getContactDimension());
            String idDateContact = idDateDimension + "_" + idContact;
            int callSum = Integer.valueOf(value.getCallSum());
            int callDurationSum = Integer.valueOf(value.getCallDurationSum());

            if (insertSQL == null) {
                insertSQL = "INSERT INTO `tb_call` (`id_date_contact`, `id_date_dimension`, `id_contact`, `call_sum`, `call_duration_sum`) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `id_date_contact` = ?;";
            }
            if (ps == null) {
                try {
                    ps = connection.prepareStatement(insertSQL);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            //本次SQL
            int i = 0;
            try {
                ps.setString(++i, idDateContact);
                ps.setInt(++i, idDateDimension);
                ps.setInt(++i, idContact);
                ps.setInt(++i, callSum);
                ps.setInt(++i, callDurationSum);
                //无则插入，有则更新的判断依据
                ps.setString(++i, idDateContact);
                ps.addBatch();
                count++;
                if (count >= BATCH_SIZE) {
                    ps.executeBatch();
                    connection.commit();
                    count = 0;
                    ps.clearBatch();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }


        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            /*
            确保最后一次 也能被提交执行
             */
            try {
                if(ps != null){
                    ps.executeBatch();
                    this.connection.commit();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
//                JDBCUtil.close(connection, ps, null);
            }
        }
    }
}
