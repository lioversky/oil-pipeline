package org.apache.spark.metrics.sink;

import com.codahale.metrics.MetricRegistry;
import com.weibo.dip.pipeline.reporter.PushGatewayReporter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Create by hongxun on 2018/8/21
 */
public class PushGatewaySink implements Sink {


  private PushGatewayReporter reporter;

  /**
   * 周期和时长
   */
  private final static int SUMMON_DEFAULT_PERIOD = 10;
  private final static String SUMMON_DEFAULT_UNIT = "SECONDS";

  private final static String SUMMON_KEY_PERIOD = "period";
  private final static String SUMMON_KEY_UNIT = "unit";


  protected int pollPeriod;
  protected TimeUnit pollUnit;


  public PushGatewaySink(Properties property, MetricRegistry registry,
      SecurityManager securityMgr) {
    String gatewayUrl = property.getProperty("url");
    String jobName = property.getProperty("job");
    if (property.containsKey(SUMMON_KEY_PERIOD)) {
      pollPeriod = Integer.parseInt(property.getProperty(SUMMON_KEY_PERIOD));
    } else {
      pollPeriod = SUMMON_DEFAULT_PERIOD;
    }

    if (property.containsKey(SUMMON_KEY_UNIT)) {
      pollUnit = TimeUnit.valueOf(property.getProperty(SUMMON_KEY_UNIT).toUpperCase());
    } else {
      pollUnit = TimeUnit.valueOf(SUMMON_DEFAULT_UNIT);
    }
    reporter = PushGatewayReporter.forRegistry(registry)
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

  @Override
  public void stop() {
    reporter.stop();
  }

  @Override
  public void report() {
    reporter.report();
  }
}
