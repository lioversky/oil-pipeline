package com.weibo.dip.pipeline.source;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Create by hongxun on 2018/7/27
 */
public abstract class DatasetSource {

  public DatasetSource(Map<String, Object> params) {
  }

  public abstract Dataset createSource(SparkSession sparkSession);
}
