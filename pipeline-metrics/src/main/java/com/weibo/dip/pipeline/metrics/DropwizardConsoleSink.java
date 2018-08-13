package com.weibo.dip.pipeline.metrics;

import com.codahale.metrics.ConsoleReporter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Create by hongxun on 2018/8/13
 */
public class DropwizardConsoleSink extends MetricsSink {

  public DropwizardConsoleSink(Properties properties) {
    super(properties);
    reporter = ConsoleReporter.forRegistry(MetricsSystem.getMetricRegistry())
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
  }

  @Override
  public void start() {
    reporter.start(pollPeriod, pollUnit);
  }

}
