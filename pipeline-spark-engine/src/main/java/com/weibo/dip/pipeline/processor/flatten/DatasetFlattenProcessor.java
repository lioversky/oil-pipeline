package com.weibo.dip.pipeline.processor.flatten;

import com.weibo.dip.pipeline.processor.DatasetProcessor;
import com.weibo.dip.pipeline.processor.FieldDatasetProcessor;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/19
 */
public class DatasetFlattenProcessor  extends DatasetProcessor {

  public DatasetFlattenProcessor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset process(Dataset data) throws Exception {
    return null;
  }
}
