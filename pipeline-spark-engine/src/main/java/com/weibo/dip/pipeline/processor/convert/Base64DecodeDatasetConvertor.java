package com.weibo.dip.pipeline.processor.convert;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.unbase64;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * base64解码器
 */
public class Base64DecodeDatasetConvertor extends DatasetConvertProcessor {

  public Base64DecodeDatasetConvertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset convert(String fieldName, Dataset dataset) {
    return dataset.withColumn(fieldName, unbase64(col(fieldName)));
  }
}
