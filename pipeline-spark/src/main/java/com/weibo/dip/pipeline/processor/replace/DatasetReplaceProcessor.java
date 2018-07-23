package com.weibo.dip.pipeline.processor.replace;

import com.weibo.dip.pipeline.processor.FieldDatasetProcessor;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/19
 */
public class DatasetReplaceProcessor extends FieldDatasetProcessor {

  private DatasetReplacer replacer;

  public DatasetReplaceProcessor(Map<String, Object> params,DatasetReplacer replacer) {
    super(params);
    this.replacer = replacer;
  }

  @Override
  public Dataset fieldProcess(Dataset data) {
    return replacer.replace(fieldName, data);
  }
}
