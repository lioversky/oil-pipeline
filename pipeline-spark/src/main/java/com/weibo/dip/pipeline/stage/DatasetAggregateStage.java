package com.weibo.dip.pipeline.stage;

import com.codahale.metrics.MetricRegistry;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/17
 */
public class DatasetAggregateStage extends Stage<Dataset>  {

  public DatasetAggregateStage(MetricRegistry metricRegistry, String stageId) {
    super(metricRegistry, stageId);
  }

  @Override
  public Dataset processStage(Dataset data) throws Exception {
    return null;
  }
}
