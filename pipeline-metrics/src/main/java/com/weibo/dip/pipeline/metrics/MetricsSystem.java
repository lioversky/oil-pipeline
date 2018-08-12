package com.weibo.dip.pipeline.metrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.concurrent.TimeUnit;

/**
 * Create by hongxun on 2018/7/3
 */
public class MetricsSystem {

  private final static MetricRegistry metricRegistry = new MetricRegistry();
  private static final ConsoleReporter reporter =
      ConsoleReporter.forRegistry(metricRegistry)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .build();

  static {
    reporter.start(1, TimeUnit.SECONDS);
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
