package com.weibo.dip.pipeline.stage;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Strings;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Create by hongxun on 2018/7/17
 */
public class DatasetAggregateStage extends Stage<Dataset> {

  private String tempTableName;
  private String sql;

  public DatasetAggregateStage(MetricRegistry metricRegistry, Map<String, Object> configMap,
      String stageId) {
    super(metricRegistry, stageId);
    if (configMap.containsKey("tempTableName")) {
      tempTableName = (String) configMap.get("tempTableName");
    }
    sql = (String) configMap.get("sql");
  }

  @Override
  public Dataset processStage(Dataset data) throws Exception {
    SparkSession sparkSession = data.sparkSession();
    if (!Strings.isNullOrEmpty(tempTableName)) {
      data.createOrReplaceTempView(tempTableName);
    }

    return sparkSession.sql(sql);
  }
}
