package com.weibo.dip.pipeline.processor;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * dataset列处理器顶层类
 * Create by hongxun on 2018/7/10
 */
public abstract class FieldDatasetProcessor extends DatasetProcessor {

  /**
   * 处理列名
   */
  protected String fieldName;

  public FieldDatasetProcessor(Map<String, Object> params) {
    super(params);
    fieldName = (String) params.get("fieldName");
  }

  @Override
  public Dataset process(Dataset data) throws Exception {
    return fieldProcess(data);
  }

  protected abstract Dataset fieldProcess(Dataset dataset);
}

