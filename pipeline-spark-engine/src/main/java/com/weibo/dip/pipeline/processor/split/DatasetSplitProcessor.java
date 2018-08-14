package com.weibo.dip.pipeline.processor.split;

import com.weibo.dip.pipeline.processor.DatasetProcessor;
import com.weibo.dip.pipeline.processor.FieldDatasetProcessor;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/19
 */
public abstract class DatasetSplitProcessor extends FieldDatasetProcessor {


  public DatasetSplitProcessor(Map<String, Object> params) {
    super(params);
  }

  abstract Dataset split(String fieldName, Dataset dataset);

  @Override
  public Dataset fieldProcess(Dataset data) {
    return split(fieldName, data);
  }
}
