package com.weibo.dip.pipeline.processor;

import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.pipeline.util.PropertiesUtil;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Create by hongxun on 2018/6/26
 */
public abstract class Processor<T> extends Configuration {

  private final static String PROPERTIES_NAME = "processors.properties";

  private final static String DEFAULT_PREFIX = PropertiesUtil.DEFAULT_PREFIX;

  private static Map<String, Properties> engineMap = PropertiesUtil.initEngineMap(PROPERTIES_NAME);

  /**
   * 按照engine，typeName获取对应的Processor类名
   *
   * 如果不包含引擎，返回默认的内容值
   *
   * @param engine 引擎名
   * @param typeName 类型名
   * @return Processor类名
   */
  public static String getProcessorClassName(String engine, String typeName) {
    return PropertiesUtil.getClassName(engineMap, engine, typeName);
  }

  /**
   * 反射创建Processor
   *
   * @param className Processor类名
   * @param params Processor参数
   * @return Processor
   * @throws Exception 异常
   */
  private static Processor createProcessor(Map<String, Object> params, String className) {
    try {
      Constructor<Processor> constructor = (Constructor<Processor>) Class.forName(className)
          .getConstructor(Map.class);
      return constructor.newInstance((Map<String, Object>) params);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Create processor error, ClassName = %s", className), e);
    }

  }

  /**
   * 按照engine名，类型名生成Processor
   *
   * @param engine 引擎名
   * @param typeName 类型名
   * @param params Processor参数
   */
  public static Processor createProcessor(String engine, String typeName,
      Map<String, Object> params) {
    String className = getProcessorClassName(engine, typeName);
    if (className == null || className.length() == 0) {
      throw new RuntimeException(
          String.format("There is no processor when engine = %s, type = %s.", engine, typeName));
    }
    return createProcessor(params, className);
  }

  public static Processor createProcessor(String typeName, Map<String, Object> params) {
    String className = getProcessorClassName(DEFAULT_PREFIX, typeName);
    if (className == null || className.length() == 0) {
      throw new RuntimeException(
          String.format("There is no processor when engine = *, type = %s.", typeName));
    }
    return createProcessor(params, className);
  }

  public Processor(Map<String, Object> params) {
    if (params != null) {
      addConfigs(params);
    }
  }

  public abstract T process(T data) throws Exception;

}
