package com.weibo.dip.pipeline.register;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * 已存在spark或者hive表，执行sql
 */
public class SparkTableExtractor extends FileTableExtractor {

  private String sql;

  /**
   * 构造函数
   *
   * @param params 参数
   */
  public SparkTableExtractor(Map<String, Object> params) {
    super(params);
    this.sql = (String) params.get("sql");
  }

  public Dataset extract(SparkSession spark) {
    return spark.sql(sql);
  }
}
