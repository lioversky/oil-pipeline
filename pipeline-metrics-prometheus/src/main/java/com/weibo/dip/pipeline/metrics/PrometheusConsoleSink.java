package com.weibo.dip.pipeline.metrics;

import com.weibo.dip.pipeline.reporter.PrometheusConsoleReporter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Create by hongxun on 2018/8/13
 */
public class PrometheusConsoleSink extends MetricsSink {

  public PrometheusConsoleSink(Properties properties) {
    super(properties);
    reporter = PrometheusConsoleReporter.forRegistry(MetricsSystem.getMetricRegistry())
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
  }

  @Override
  public void start() {
    reporter.start(pollPeriod, pollUnit);
  }

}
