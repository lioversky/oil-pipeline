package com.weibo.dip.pipeline.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

/**
 * Create by hongxun on 2018/7/3
 */
public class MetricSystem {

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  public void setMetricRegistry(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  private MetricRegistry metricRegistry;

  private static MetricSystem system = new MetricSystem();

  private MetricSystem() {
    metricRegistry = new MetricRegistry();
  }

  public static MetricSystem getMetricSystem() {
    return system;
  }

  public void registry(String name, Metric m) {
    metricRegistry.register(name, m);
  }
}
