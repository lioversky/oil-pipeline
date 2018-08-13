package com.weibo.dip.pipeline.metrics;

import com.codahale.metrics.ScheduledReporter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Create by hongxun on 2018/8/13
 */
public abstract class MetricsSink {

  protected ScheduledReporter reporter;
  private final static int SUMMON_DEFAULT_PERIOD = 10;
  private final static String SUMMON_DEFAULT_UNIT = "SECONDS";

  private final static String SUMMON_KEY_PERIOD = "period";
  private final static String SUMMON_KEY_UNIT = "unit";


  protected int pollPeriod;
  protected TimeUnit pollUnit;

  public MetricsSink(Properties properties) {
    if (properties.containsKey(SUMMON_KEY_PERIOD)) {
      pollPeriod = Integer.parseInt(properties.getProperty(SUMMON_KEY_PERIOD));
    } else {
      pollPeriod = SUMMON_DEFAULT_PERIOD;
    }

    if (properties.containsKey(SUMMON_KEY_UNIT)) {
      pollUnit = TimeUnit.valueOf(properties.getProperty(SUMMON_KEY_UNIT).toUpperCase());
    } else {
      pollUnit = TimeUnit.valueOf(SUMMON_DEFAULT_UNIT);
    }
  }

  public abstract void start();

  public void report() {
    if (reporter != null) {
      reporter.report();
    }
  }

  public void stop() {
    if (reporter != null) {
      reporter.stop();
    }
  }
}
