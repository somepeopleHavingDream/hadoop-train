package org.yangxin.hadoop;

import java.io.IOException;
import java.util.Properties;

/**
 * 读取属性配置文件
 *
 * @author yangxin
 * 2023/7/3 13:00
 */
public class ParamUtil {

    private static final Properties properties = new Properties();

    static {
        try {
            properties.load(ParamUtil.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties() {
        return properties;
    }

    public static void main(String[] args) {
        System.out.println(getProperties().getProperty("INPUT_PATH"));
    }
}
