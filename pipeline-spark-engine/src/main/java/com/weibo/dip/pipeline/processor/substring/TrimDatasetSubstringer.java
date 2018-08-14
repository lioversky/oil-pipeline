package com.weibo.dip.pipeline.processor.substring;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.trim;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * 截空格
 */
public class TrimDatasetSubstringer extends DatasetSubstringProcessor {


  public TrimDatasetSubstringer(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset substring(String fieldName,Dataset dataset) {
    return dataset.withColumn(fieldName, trim(col(fieldName)));
  }
}
