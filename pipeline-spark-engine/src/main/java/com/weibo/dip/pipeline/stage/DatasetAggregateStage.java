package com.weibo.dip.pipeline.stage;

import com.google.common.base.Strings;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * spark 聚合stage
 * Create by hongxun on 2018/7/17
 */
public class DatasetAggregateStage extends Stage<Dataset> {

  /**
   * 对dataset创建的临时表名
   */
  private String tempTableName;
  /**
   * 执行的sql语句
   */
  private String sql;

  public DatasetAggregateStage(Map<String, Object> configMap,
      String stageId) {
    super(stageId);
    if (configMap.containsKey("tempTableName")) {
      tempTableName = (String) configMap.get("tempTableName");
    }
    sql = (String) configMap.get("sql");
  }

  @Override
  public Dataset processStage(Dataset data) {
    SparkSession sparkSession = data.sparkSession();
    if (!Strings.isNullOrEmpty(tempTableName)) {
      data.createOrReplaceTempView(tempTableName);
    }

    return sparkSession.sql(sql);
  }
}
