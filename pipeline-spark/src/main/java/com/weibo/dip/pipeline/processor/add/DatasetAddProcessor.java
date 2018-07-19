package com.weibo.dip.pipeline.processor.add;

import com.weibo.dip.pipeline.processor.FieldDatasetProcessor;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/19
 */
public class DatasetAddProcessor  extends FieldDatasetProcessor {

  @Override
  public Dataset fieldProcess(Dataset data) {
    return null;
  }

  public DatasetAddProcessor(Map<String, Object> params) {
    super(params);
  }
}
