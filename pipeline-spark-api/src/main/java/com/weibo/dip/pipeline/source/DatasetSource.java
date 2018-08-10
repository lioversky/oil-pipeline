package com.weibo.dip.pipeline.source;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * 读取DatasetSource的抽象类
 * Create by hongxun on 2018/7/27
 */
public abstract class DatasetSource extends Source {

  public DatasetSource(Map<String, Object> params) {
  }

  public abstract Dataset createSource(SparkSession sparkSession);
}
