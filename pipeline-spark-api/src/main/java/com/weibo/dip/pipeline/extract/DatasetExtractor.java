package com.weibo.dip.pipeline.extract;

import java.io.Serializable;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * dataset提取器
 * Create by hongxun on 2018/7/19
 */
public abstract class DatasetExtractor extends Extractor<Dataset> {

  protected String fieldName = "_value_";

  public DatasetExtractor(Map<String, Object> params) {
    super(params);
    if (params.containsKey("fieldName")) {
      fieldName = (String) params.get("fieldName");
    }
  }

  @Override
  public Dataset extract(Object data) throws Exception {
    return extract((Dataset) data);
  }

  public abstract Dataset extract(Dataset dataset);
}
