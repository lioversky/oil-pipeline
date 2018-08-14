package com.weibo.dip.pipeline.processor.convert;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.md5;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * md5转换器
 */
public class MD5DatasetConvertor extends DatasetConvertProcessor {

  public MD5DatasetConvertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset convert(String fieldName, Dataset dataset) {
    return dataset.withColumn(fieldName, md5(col(fieldName)));
  }
}
