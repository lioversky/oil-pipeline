package com.weibo.dip.pipeline.metrics;

import com.weibo.dip.pipeline.reporter.PushGatewayReporter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Create by hongxun on 2018/8/13
 */
public class PushGatewaySink extends MetricsSink {

  public PushGatewaySink(Properties properties) {
    super(properties);
    reporter = PushGatewayReporter.forRegistry(MetricsSystem.getMetricRegistry())
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
  }

  @Override
  public void start() {
    reporter.start(pollPeriod, pollUnit);
  }
}
