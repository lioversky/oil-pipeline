package com.weibo.dip.pipeline.processor.substring;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * 左右子串
 */
public class LRDatasetSubstringer extends DatasetSubstringProcessor {

  private Integer left;
  private Integer right;

  @Override
  public Dataset substring(String fieldName,Dataset dataset) {
    return dataset
        .withColumn(fieldName, callUDF("substring_lr", col(fieldName), lit(left), lit(right)));
  }

  public LRDatasetSubstringer(Map<String, Object> params) {
    super(params);
    this.left = params.containsKey("left") ? ((Number) params.get("left")).intValue() : 0;
    this.right = params.containsKey("right") ? ((Number) params.get("right")).intValue() : -1;
  }
}
