package com.weibo.dip.pipeline.processor.replace;

import com.weibo.dip.pipeline.processor.FieldDatasetProcessor;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/19
 */
public abstract class DatasetReplaceProcessor extends FieldDatasetProcessor {

  public DatasetReplaceProcessor(Map<String, Object> params) {
    super(params);
  }

  abstract Dataset replace(String fieldName, Dataset dataset);

  @Override
  public Dataset fieldProcess(Dataset data) {
    return replace(fieldName, data);
  }
}
