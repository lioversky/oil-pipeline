package com.weibo.dip.pipeline.processor.substring;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.substring_index;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * 分隔索引子串
 */
public class IndexDatasetSubstringer extends DatasetSubstringProcessor {

  private String delim;
  private int count;

  public IndexDatasetSubstringer(Map<String, Object> params) {
    super(params);
    delim = (String) params.get("delim");
    count = (Integer) params.get("count");
  }

  @Override
  public Dataset substring(String fieldName,Dataset dataset) {
    return dataset.withColumn(fieldName, substring_index(col(fieldName), delim, count));
  }
}
