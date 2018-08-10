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


  public static String getValue(String key) {
    return properties.getProperty(key, "");
  }

  public static void main(String[] args) {
    System.out.println(getValue("regex"));
  }
}
