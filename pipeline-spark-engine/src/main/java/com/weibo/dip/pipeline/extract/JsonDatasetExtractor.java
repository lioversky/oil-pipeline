package com.weibo.dip.pipeline.extract;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * json提取
 */
public class JsonDatasetExtractor extends DatasetExtractor {

  public JsonDatasetExtractor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset extract(Dataset dataset) {
    return null;
  }
}
