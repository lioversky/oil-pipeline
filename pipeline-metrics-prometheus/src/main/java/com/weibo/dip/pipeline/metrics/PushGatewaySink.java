package com.weibo.dip.pipeline.metrics;

import com.weibo.dip.pipeline.reporter.PushGatewayReporter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Prometheus PushGateway输出实现类，需要指定PushGateway url和jobName
 * Create by hongxun on 2018/8/13
 */
public class PushGatewaySink extends MetricsSink {

  public PushGatewaySink(Properties properties) {
    super(properties);
    String gatewayUrl = properties.getProperty("url");
    String jobName = properties.getProperty("job");
    reporter = PushGatewayReporter.forRegistry(MetricsSystem.getMetricRegistry())
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .withGatewayUrl(gatewayUrl)
        .withJobName(jobName)
        .build();
  }

  @Override
  public void start() {
    reporter.start(pollPeriod, pollUnit);
  }
}
