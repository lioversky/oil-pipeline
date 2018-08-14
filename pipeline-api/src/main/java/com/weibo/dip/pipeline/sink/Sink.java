package com.weibo.dip.pipeline.sink;

import com.weibo.dip.pipeline.Sequence;
import com.weibo.dip.pipeline.Step;
import com.weibo.dip.pipeline.util.PropertiesUtil;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * sink顶层抽象类.
 * 计划输出包括：hdfs,console,kafka,summon(es),influxdb,db,http
 * Create by hongxun on 2018/7/6
 */

public abstract class Sink<T> extends Step implements Sequence {

  protected String metricsName;

  private static final AtomicInteger INDEX = new AtomicInteger();

  private static final String PROPERTIES_NAME = "sinks.properties";

  private static final String DEFAULT_PREFIX = PropertiesUtil.DEFAULT_PREFIX;

  private static Map<String, Properties> engineMap = PropertiesUtil.initEngineMap(PROPERTIES_NAME);

  public static String getSinkClassName(String engine, String typeName) {
    return PropertiesUtil.getClassName(engineMap, engine, typeName);
  }

  private static Sink createSink(Map<String, Object> params, String className) {
    try {
      Constructor<Sink> constructor = (Constructor<Sink>) Class.forName(className)
          .getConstructor(Map.class);
      return constructor.newInstance((Map<String, Object>) params);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Create sink error, ClassName = %s", className), e);
    }
  }

  public static Sink createSink(String engine, String typeName,
      Map<String, Object> params) {
    String className = getSinkClassName(engine, typeName);
    if (className == null || className.length() == 0) {
      throw new RuntimeException(
          String.format("There is no sink when engine = %s, type = %s.", engine, typeName));
    }
    return createSink(params, className);
  }

  public static Sink createSink(String typeName, Map<String, Object> params) {
    String className = getSinkClassName(DEFAULT_PREFIX, typeName);
    if (className == null || className.length() == 0) {
      throw new RuntimeException(
          String.format("There is no sink when engine = *, type = %s.", typeName));
    }
    return createSink(params, className);
  }

  public Sink(Map<String, Object> params) {
    metricsName = getClass().getSimpleName() + "_" + getSequence();
  }

  @Override
  public int getSequence() {
    return INDEX.incrementAndGet();
  }

  /**
   * 写出数据
   *
   * @param t 类型由各实现类指定
   */
  public abstract void write(T t);

  /**
   * 关闭资源方法，如无资源关闭，空实现
   */
  public abstract void stop();
}
