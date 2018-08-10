package com.weibo.dip.pipeline.extract;

import com.weibo.dip.pipeline.util.PropertiesUtil;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Create by hongxun on 2018/7/4
 */
public abstract class Extractor<T> implements Serializable {

  private final static String DEFAULT_PREFIX = PropertiesUtil.DEFAULT_PREFIX;

  private final static String PROPERTIES_NAME = "extractors.properties";
  private static Map<String, Properties> engineMap = PropertiesUtil.initEngineMap(PROPERTIES_NAME);


  public static String getExtractorClassName(String engine, String typeName) {
    return PropertiesUtil.getClassName(engineMap, engine, typeName);
  }

  private static Extractor createExtractor(Map<String, Object> params, String className) {
    try {
      Constructor<Extractor> constructor = (Constructor<Extractor>) Class.forName(className)
          .getConstructor(Map.class);
      return constructor.newInstance((Map<String, Object>) params);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Create Extractor error, ClassName = %s", className), e);
    }

  }

  public static Extractor createExtractor(String engine, Map<String, Object> params) {
    String typeName = (String) params.get("type");
    String className = getExtractorClassName(engine, typeName);
    if (className == null || className.length() == 0) {
      throw new RuntimeException(
          String.format("There is no processor when engine = %s, type = %s.", engine, typeName));
    }
    return createExtractor(params, className);
  }

  public static Extractor createExtractor(Map<String, Object> params) {
    String typeName = (String) params.get("type");
    String className = getExtractorClassName(DEFAULT_PREFIX, typeName);
    if (className == null || className.length() == 0) {
      throw new RuntimeException(
          String.format("There is no processor when engine = *, type = %s.", typeName));
    }
    return createExtractor(params, className);
  }


  public Extractor(Map<String, Object> jsonMap) {
  }

  private static final long serialVersionUID = 1L;
  protected String[] columns;

  public abstract T extract(Object data) throws Exception;
}

