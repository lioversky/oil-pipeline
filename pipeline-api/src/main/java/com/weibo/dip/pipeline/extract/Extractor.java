package com.weibo.dip.pipeline.extract;

import com.weibo.dip.pipeline.Sequence;
import com.weibo.dip.pipeline.util.PropertiesUtil;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Create by hongxun on 2018/7/4
 */
public abstract class Extractor<T> implements Sequence {

  protected String metricsName;
  private final static String DEFAULT_PREFIX = PropertiesUtil.DEFAULT_PREFIX;
  private final static AtomicInteger index = new AtomicInteger();
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
          String.format("Create extractor error, ClassName = %s", className), e);
    }

  }

  public static Extractor createExtractor(String engine, String typeName,
      Map<String, Object> params) {
    String className = getExtractorClassName(engine, typeName);
    if (className == null || className.length() == 0) {
      throw new RuntimeException(
          String.format("There is no extractor when engine = %s, type = %s.", engine, typeName));
    }
    return createExtractor(params, className);
  }

  public static Extractor createExtractor(String engine, Map<String, Object> params) {
    String typeName = (String) params.get("type");
    String className = getExtractorClassName(engine, typeName);
    if (className == null || className.length() == 0) {
      throw new RuntimeException(
          String.format("There is no extractor when engine = %s, type = %s.", engine, typeName));
    }
    return createExtractor(params, className);
  }

  public static Extractor createExtractor(Map<String, Object> params) {
    String typeName = (String) params.get("type");
    String className = getExtractorClassName(DEFAULT_PREFIX, typeName);
    if (className == null || className.length() == 0) {
      throw new RuntimeException(
          String.format("There is no extractor when engine = *, type = %s.", typeName));
    }
    return createExtractor(params, className);
  }


  public Extractor(Map<String, Object> jsonMap) {
    metricsName = getClass().getSimpleName() + "_" + getSequence();
  }

  private static final long serialVersionUID = 1L;
  protected String[] columns;

  public abstract T extract(Object data) ;

  @Override
  public int getSequence() {
    return index.incrementAndGet();
  }
}

