package com.weibo.dip.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 获取最基本配置
 * Created by hongxun on 17/1/22.
 */
public class PropertiesUtil {

  private static InputStream _input = null;
  private static Properties properties = new Properties();

//  static {
//    _input = PropertiesUtil.class.getClassLoader().getResourceAsStream("common.properties");
//    try {
//      properties.load(_input);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }

  public static Properties load(String file) {
    InputStream inputStream = PropertiesUtil.class.getClassLoader().getResourceAsStream(file);
    Properties properties = new Properties();
    try {
      properties.load(inputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return properties;
  }

  /**
   * 通过属性名截取
   *
   * @param properties 原始kv
   * @param pattern 匹配正则
   * @return 截取健值
   */

  public static Map<String, Properties> subProperties(Properties properties,
      Pattern pattern) {
    Map<String, Properties> subProperties = new HashMap<>();
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      Matcher matcher = pattern.matcher((String) entry.getKey());
      if (matcher.find() && matcher.groupCount() == 2) {
        String prefix = matcher.group(1);
        String suffix = matcher.group(2);
        Properties prop;
        if (subProperties.containsKey(prefix)) {
          prop = subProperties.get(prefix);
        } else {
          prop = new Properties();
          subProperties.put(prefix, prop);
        }
        prop.put(suffix, entry.getValue());
      }
    }
    return subProperties;
  }


  public static String getValue(String key) {
    return properties.getProperty(key, "");
  }

  public static void main(String[] args) {
    System.out.println(getValue("regex"));
  }
}
