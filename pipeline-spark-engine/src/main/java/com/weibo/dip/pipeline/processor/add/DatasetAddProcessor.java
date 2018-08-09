package com.weibo.dip.pipeline.processor.add;

import com.weibo.dip.pipeline.processor.FieldDatasetProcessor;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/19
 */
public abstract class DatasetAddProcessor extends FieldDatasetProcessor {

  protected String targetField;

  @Override
  public Dataset fieldProcess(Dataset data) {
    return add(data);
  }

  public DatasetAddProcessor(Map<String, Object> params) {
    super(params);
    targetField = (String) params.get("targetField");
  }

  abstract Dataset add(Dataset dataset);
}
