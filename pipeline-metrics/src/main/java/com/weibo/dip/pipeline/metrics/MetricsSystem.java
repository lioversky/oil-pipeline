package com.weibo.dip.pipeline.metrics;

import com.codahale.metrics.ConsoleReporter;
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
import java.util.concurrent.TimeUnit;

/**
 * Create by hongxun on 2018/7/3
 */
public class MetricsSystem {

  private final static MetricRegistry metricRegistry = new MetricRegistry();


  private final static String PROPERTIES_NAME = "metrics.properties";


  private static Map<String, Properties> sinkMap = PropertiesUtil.initEngineMap(PROPERTIES_NAME);


  static {
    for (Map.Entry<String, Properties> entry : sinkMap.entrySet()) {
      Properties value = entry.getValue();
      try {
        if (value.containsKey("class")) {
          String className = value.getProperty("class");
          Constructor<MetricsSink> constructor = (Constructor<MetricsSink>) Class.forName(className)
              .getConstructor(Properties.class);
          MetricsSink sink = constructor.newInstance(value);
          sink.start();
        }
      } catch (Exception e) {

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

  public static Meter getMeter(String meterName) {
    return metricRegistry.meter(meterName);
  }

  public static Counter getCounter(String counterName) {
    return metricRegistry.counter(counterName);
  }

  public static Timer getTimer(String timerName) {
    return metricRegistry.timer(timerName);
  }

  public static Histogram getHistogram(String histogramName) {
    return metricRegistry.histogram(histogramName);
  }

}
