package com.weibo.dip.pipeline.processor.substring;

import com.weibo.dip.pipeline.processor.FieldDatasetProcessor;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/19
 */
public abstract class DatasetSubstringProcessor extends FieldDatasetProcessor {

  public DatasetSubstringProcessor(Map<String, Object> params) {
    super(params);
  }

  abstract Dataset substring(String fieldName, Dataset dataset);

  @Override
  public Dataset fieldProcess(Dataset dataset) {
    return substring(fieldName, dataset);
  }
}
