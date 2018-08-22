package org.apache.spark.metrics.source;

import com.codahale.metrics.MetricRegistry;
import com.weibo.dip.pipeline.metrics.MetricsSystem;

/**
 * Create by hongxun on 2018/8/21
 */
public class PipelineSource implements Source {

  //  val executorId = SparkEnv.get.executorId;
  @Override
  public String sourceName() {
    return "pipeline";
  }

  @Override
  public MetricRegistry metricRegistry() {
    return MetricsSystem.getMetricRegistry();
  }
}
