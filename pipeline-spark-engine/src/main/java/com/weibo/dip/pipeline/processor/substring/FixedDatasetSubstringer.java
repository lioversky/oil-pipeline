package com.weibo.dip.pipeline.processor.substring;

import static org.apache.spark.sql.functions.col;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;

/**
 * 定长子串
 */
public class FixedDatasetSubstringer extends DatasetSubstringProcessor {

  private int start;
  private int length;

  @Override
  public Dataset substring(String fieldName,Dataset dataset) {
    return dataset.withColumn(fieldName, functions.substring(col(fieldName), start, length));
  }

  public FixedDatasetSubstringer(Map<String, Object> params) {
    super(params);
    this.start = params.containsKey("start") ? ((Number) params.get("start")).intValue() : 0;
    this.length = params.containsKey("length") ? ((Number) params.get("length")).intValue() : -1;
  }
}
