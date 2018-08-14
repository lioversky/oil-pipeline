package com.weibo.dip.pipeline.source;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * 读取DatasetSource的抽象类
 * Create by hongxun on 2018/7/27
 */
public abstract class DatasetSource extends Source {

  /**
   * 输入源类型
   */
  protected String sourceFormat;
  /**
   * 源配置
   */
  protected Map<String, String> sourceOptions;

  public DatasetSource(Map<String, Object> params) {
    sourceFormat = (String) params.get("format");
    sourceOptions = (Map<String, String>) params.get("options");
  }

  public abstract Dataset createSource(SparkSession sparkSession);
}
