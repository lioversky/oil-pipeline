package com.weibo.dip.pipeline.processor.split;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/8/8
 */
public class JsonDatasetSplitter extends DatasetSplitProcessor {

  public JsonDatasetSplitter(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset split(String fieldName, Dataset dataset) {
    return null;
  }
}
