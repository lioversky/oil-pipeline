package com.weibo.dip.pipeline.metrics;

import com.codahale.metrics.ScheduledReporter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * metrics输出抽象类
 * Create by hongxun on 2018/8/13
 */
public abstract class MetricsSink {

  /**
   * metrics reporter，由子类生成具体实例
   */
  protected ScheduledReporter reporter;
  /**
   * 周期和时长
   */
  private final static int SUMMON_DEFAULT_PERIOD = 10;
  private final static String SUMMON_DEFAULT_UNIT = "SECONDS";

  private final static String SUMMON_KEY_PERIOD = "period";
  private final static String SUMMON_KEY_UNIT = "unit";


  protected int pollPeriod;
  protected TimeUnit pollUnit;

  /**
   * 构造方法中获取周期和时长或默认参数
   */
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

  /**
   * 由子类实现，start reporter
   */
  public abstract void start();

  /**
   * 此方法供停止时有未report的数据
   */
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
