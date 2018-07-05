package mapper;

import kv.key.CommonDimension;
import kv.key.ContactDimension;
import kv.key.DateDimension;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 16:20 2018/7/1
 * @Description:
 */

public class CountDurationMapper extends TableMapper<CommonDimension,Text> {

    private CommonDimension commonDimension = new CommonDimension();
    private Text durationText = new Text();
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //这里面是rowKey
        String rowKey = Bytes.toString(key.get());
        String[] splits = rowKey.split("_");
        //这是flag的数据  意思是现在只处理 主叫数据
        if (splits[4].equals("0")) {
            return;
        }
        String caller = splits[1];
        String callee = splits[3];
        String buildTime = splits[2];
        String duration = splits[5];
        durationText.set(duration);
        String year  =  buildTime.substring(0, 4);
        String month =  buildTime.substring(4, 6);
        String day   =   buildTime.substring(4, 6);

        //组装commonDimension
        DateDimension yearDimension = new DateDimension(year, "-1", "-1");
        DateDimension monthDimension = new DateDimension(year, month, "-1");
        DateDimension dayDimension = new DateDimension(year, month, day);
        //组装contactDimension
        ContactDimension callerContactDimension = new ContactDimension(caller, "");

        //聚合主叫数据
        //年
        commonDimension.setContactDimension(callerContactDimension);
        commonDimension.setDateDimension(yearDimension);
        context.write(commonDimension, durationText);
        //月
        commonDimension.setDateDimension(monthDimension);
        context.write(commonDimension, durationText);
        //日
        commonDimension.setDateDimension(dayDimension);
        context.write(commonDimension, durationText);

        //聚合被叫数据
        //年
        ContactDimension calleeContactDimension = new ContactDimension(callee, "");
        commonDimension.setContactDimension(calleeContactDimension);

        commonDimension.setDateDimension(yearDimension);
        context.write(commonDimension, durationText);
        //月
        commonDimension.setDateDimension(monthDimension);
        context.write(commonDimension, durationText);
        //日
        commonDimension.setDateDimension(dayDimension);
        context.write(commonDimension, durationText);

    }

}
