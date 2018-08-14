package com.weibo.dip.pipeline.processor.convert;

import static org.apache.spark.sql.functions.base64;
import static org.apache.spark.sql.functions.col;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * base64转码器
 */
public class Base64EncodeDatasetConvertor extends DatasetConvertProcessor {

  public Base64EncodeDatasetConvertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset convert(String fieldName, Dataset dataset) {
    return dataset.withColumn(fieldName, base64(col(fieldName)));
  }
}
