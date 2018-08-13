package com.weibo.dip.pipeline.util;

import com.weibo.dip.pipeline.processor.Processor;
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

  public static Properties load(String file) {
    InputStream inputStream = Processor.class.getClassLoader().getResourceAsStream(file);
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

  public final static String DEFAULT_PREFIX = "*";
  public final static Pattern ENGINE_REGEX_PATTERN = Pattern.compile("^(\\*|[a-zA-Z_]+)\\.(.+)");

  public static Map<String, Properties> initEngineMap(String propertiesName) {

    Properties properties = PropertiesUtil.load(propertiesName);
    Map<String, Properties> engineMap = PropertiesUtil.subProperties(properties,
        ENGINE_REGEX_PATTERN);
    //将默认*的内容附加到所有的engine中
    if (engineMap.containsKey(DEFAULT_PREFIX)) {
      Properties defaultSubProperties = engineMap.get(DEFAULT_PREFIX);
      for (Map.Entry<String, Properties> entry : engineMap.entrySet()) {
        if (DEFAULT_PREFIX.equals(entry.getKey())) {
          continue;
        } else {
          for (Map.Entry<Object, Object> defaultEntry : defaultSubProperties.entrySet()) {
            if (!entry.getValue().containsKey(defaultEntry.getKey())) {
              entry.getValue().put(defaultEntry.getKey(), defaultEntry.getValue());
            }
          }
        }
      }
    }
    return engineMap;
  }

  public static String getClassName(Map<String, Properties> engineMap,String engine, String typeName){
    if (engineMap.containsKey(engine)) {
      Properties typeProperties = engineMap.get(engine);
      if (typeProperties.containsKey(typeName)) {
        return typeProperties.getProperty(typeName);
      }
    } else {
      if (engineMap.containsKey(DEFAULT_PREFIX)) {
        return engineMap.get(DEFAULT_PREFIX).getProperty(typeName);
      }
    }
    return null;
  }
}
