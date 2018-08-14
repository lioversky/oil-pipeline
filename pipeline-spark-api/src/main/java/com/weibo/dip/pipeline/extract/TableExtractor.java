package com.weibo.dip.pipeline.extract;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * 文件内容提取器
 * Create by hongxun on 2018/7/25
 */
public abstract class TableExtractor implements Serializable {

  public TableExtractor(Map<String, Object> params) {

  }

  public abstract Dataset extract(SparkSession spark);


}
