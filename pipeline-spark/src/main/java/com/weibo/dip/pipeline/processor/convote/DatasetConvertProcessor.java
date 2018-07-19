package com.weibo.dip.pipeline.processor.convote;

import com.weibo.dip.pipeline.processor.FieldDatasetProcessor;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/19
 */
public class DatasetConvertProcessor extends FieldDatasetProcessor {

  public DatasetConvertProcessor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset fieldProcess(Dataset data) {
    return null;
  }
}
