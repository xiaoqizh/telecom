package kafka;

import hbase.HBaseDAO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.PropertiesUtil;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 19:35 2018/6/29
 * @Description:
 */

public class HBaseConsumer {
    public static void main(String[] args) throws ParseException {
        /*
        kafka消费到HBase中
        这是新版本比较简单的API
         */
        HBaseDAO hBaseDAO = new HBaseDAO();

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(PropertiesUtil.properties);
        //设置主题
        kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("kafka.topics")));
        while (true) {
            //拉取消费
            /*
             * 这里是可以进行缓存的   默认是来一条就输出一条
             */
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                hBaseDAO.put(value);
                System.out.println(value);
            }
        }
    }
}
