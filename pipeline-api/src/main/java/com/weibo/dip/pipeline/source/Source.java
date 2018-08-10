package com.weibo.dip.pipeline.source;

import com.weibo.dip.pipeline.Step;
import com.weibo.dip.pipeline.util.PropertiesUtil;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;

/**
 * source的顶层类，由于不同source的创建参数不同，暂时未定义抽象方法
 * Create by hongxun on 2018/7/6
 */

public abstract class Source extends Step {

  private final static String PROPERTIES_NAME = "sources.properties";

  private final static String DEFAULT_PREFIX = PropertiesUtil.DEFAULT_PREFIX;

  private static Map<String, Properties> engineMap = PropertiesUtil.initEngineMap(PROPERTIES_NAME);

  public static String getSourceClassName(String engine, String typeName) {
    return PropertiesUtil.getClassName(engineMap, engine, typeName);
  }

  private static Source createSource(Map<String, Object> params, String className) {
    try {
      Constructor<Source> constructor = (Constructor<Source>) Class.forName(className)
          .getConstructor(Map.class);
      return constructor.newInstance((Map<String, Object>) params);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Create source error, ClassName = %s", className), e);
    }
  }

  public static Source createSource(String engine, String typeName,
      Map<String, Object> params) {
    String className = getSourceClassName(engine, typeName);
    if (className == null || className.length() == 0) {
      throw new RuntimeException(
          String.format("There is no source when engine = %s, type = %s.", engine, typeName));
    }
    return createSource(params, className);
  }

  public static Source createSource(String typeName, Map<String, Object> params) {
    String className = getSourceClassName(DEFAULT_PREFIX, typeName);
    if (className == null || className.length() == 0) {
      throw new RuntimeException(
          String.format("There is no source when engine = *, type = %s.", typeName));
    }
    return createSource(params, className);
  }

  public abstract void stop();
}
