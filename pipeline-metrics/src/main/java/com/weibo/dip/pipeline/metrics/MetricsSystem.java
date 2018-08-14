package com.weibo.dip.pipeline.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.weibo.dip.pipeline.util.PropertiesUtil;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 全局静态类，可获取Metrics的处理实例，并初始化所有MetricsSink
 * Create by hongxun on 2018/7/3
 */
public class MetricsSystem {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsSystem.class);
  private final static MetricRegistry metricRegistry = new MetricRegistry();

  /**
   * metrics配置文件名称
   */
  private final static String PROPERTIES_NAME = "metrics.properties";

  /**
   * 各输出名和对应参数
   */
  private static Map<String, Properties> sinkMap = PropertiesUtil.initEngineMap(PROPERTIES_NAME);


  /**
   * 加载所有配置的MetricsSink子类并启动
   */
  static {
    for (Map.Entry<String, Properties> entry : sinkMap.entrySet()) {
      Properties value = entry.getValue();
      String className = value.getProperty("class");
      try {

        if (className != null && className.trim().length() != 0) {
          Constructor<MetricsSink> constructor = (Constructor<MetricsSink>) Class.forName(className)
              .getConstructor(Properties.class);
          MetricsSink sink = constructor.newInstance(value);
          sink.start();
        } else {
          LOGGER.error(String.format("Start MetricsSink error, class is null."));
        }
      } catch (Exception e) {
        LOGGER.error(String.format("Start MetricsSink error, className = %s", className));
      }
    }
  }

  public static MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }


  private static MetricsSystem system = new MetricsSystem();

  private MetricsSystem() {

  }

  public static MetricsSystem getMetricSystem() {
    return system;
  }

  public static void registry(String name, Metric m) {
    metricRegistry.register(name, m);
  }

  /**
   * 获取Meter，如果不存在创建，有则使用
   *
   * @param meterName meter名称
   */
  public static Meter getMeter(String meterName) {
    return metricRegistry.meter(meterName);
  }

  /**
   * 获取Counter，如果不存在创建，有则使用
   *
   * @param counterName Counter名称
   */
  public static Counter getCounter(String counterName) {
    return metricRegistry.counter(counterName);
  }

  /**
   * 获取Timer，如果不存在创建，有则使用
   *
   * @param timerName 名称
   */
  public static Timer getTimer(String timerName) {
    return metricRegistry.timer(timerName);
  }

  /**
   * Histogram，如果不存在创建，有则使用
   *
   * @param histogramName 名称
   */
  public static Histogram getHistogram(String histogramName) {
    return metricRegistry.histogram(histogramName);
  }

}
