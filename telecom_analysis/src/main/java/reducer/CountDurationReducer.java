package reducer;

import kv.key.CommonDimension;
import kv.value.CountDurationValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 21:48 2018/7/1
 * @Description:
 */

public class CountDurationReducer extends Reducer<CommonDimension,Text,CommonDimension,CountDurationValue> {

    private CountDurationValue countDurationValue = new CountDurationValue();

    @Override
    protected void reduce(CommonDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int callSum = 0;
        int callDurationSum = 0;
        for (Text t : values) {
            callSum++;
            callDurationSum += Integer.valueOf(t.toString());
        }
        countDurationValue.setCallSum(String.valueOf(callSum));
        countDurationValue.setCallDurationSum(String.valueOf(callDurationSum));
        context.write(key, countDurationValue);
    }
}
