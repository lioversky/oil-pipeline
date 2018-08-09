package com.weibo.dip.pipeline.processor.select;

import com.weibo.dip.pipeline.processor.DatasetProcessor;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/19
 */
public class DatasetRemoveProcessor extends DatasetProcessor {

  public DatasetRemoveProcessor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset process(Dataset data) throws Exception {
    return null;
  }
}
