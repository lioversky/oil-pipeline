package com.weibo.dip.pipeline.processor.substring;

import com.weibo.dip.pipeline.processor.FieldDatasetProcessor;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/19
 */
public class DatasetSubstringProcessor extends FieldDatasetProcessor {

  private DatasetSubstringer substringer;

  public DatasetSubstringProcessor(Map<String, Object> params, DatasetSubstringer substringer) {
    super(params);
    this.substringer = substringer;
  }

  @Override
  public Dataset fieldProcess(Dataset dataset) {
    return substringer.substring(fieldName, dataset);
  }
}
