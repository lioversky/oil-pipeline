package com.weibo.dip.pipeline.processor.split;

import com.weibo.dip.pipeline.processor.DatasetProcessor;
import com.weibo.dip.pipeline.processor.FieldDatasetProcessor;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/19
 */
public class DatasetSplitProcessor extends FieldDatasetProcessor {

  private DatasetSpliter spliter;

  public DatasetSplitProcessor(Map<String, Object> params, DatasetSpliter datasetSpliter) {
    super(params);
    spliter = datasetSpliter;
  }

  @Override
  public Dataset fieldProcess(Dataset data) {
    return spliter.split(fieldName, data);
  }
}
