package utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 19:44 2018/6/29
 * @Description:
 */

public class PropertiesUtil {
    public     static   Properties properties = null;
    static{
        try {
            // 加载配置属性
            InputStream inputStream =
                    new FileInputStream("D:\\ideaCode\\telecom\\telecom_consumer\\src\\main\\resources\\hbase_consumer.properties");
            properties = new Properties();
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key){
        return properties.getProperty(key);
    }
}
