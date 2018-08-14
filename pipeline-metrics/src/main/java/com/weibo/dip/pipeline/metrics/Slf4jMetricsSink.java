package com.weibo.dip.pipeline.metrics;

import com.codahale.metrics.Slf4jReporter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create by hongxun on 2018/8/14
 */
public class Slf4jMetricsSink extends MetricsSink {


  private static final Logger LOGGER = LoggerFactory.getLogger("metrics");

  public Slf4jMetricsSink(Properties properties) {
    super(properties);
    reporter = Slf4jReporter.forRegistry(MetricsSystem.getMetricRegistry())
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .outputTo(LOGGER)
        .build();
  }

  @Override
  public void start() {
    reporter.start(pollPeriod, pollUnit);
  }
}
