package com.weibo.dip.pipeline.extract;

import java.io.Serializable;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * dataset提取器
 * Create by hongxun on 2018/7/19
 */
public abstract class DatasetExtractor implements Serializable {

  protected String fieldName = "_value_";

  public DatasetExtractor(Map<String, Object> params) {
    if (params.containsKey("fieldName")) {
      fieldName = (String) params.get("fieldName");
    }
  }

  public abstract Dataset extract(Dataset dataset);
}
