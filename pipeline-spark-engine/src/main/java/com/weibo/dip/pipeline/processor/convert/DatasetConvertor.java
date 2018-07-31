package com.weibo.dip.pipeline.processor.convert;

import static org.apache.spark.sql.functions.base64;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.md5;
import static org.apache.spark.sql.functions.unbase64;

import com.weibo.dip.pipeline.configuration.Configuration;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;

/**
 * dataset列转换器
 * Create by hongxun on 2018/7/23
 */
public abstract class DatasetConvertor extends Configuration {

  public DatasetConvertor(Map<String, Object> params) {
  }

  public abstract Dataset convert(String fieldName, Dataset dataset);
}

/**
 * url参数转换成对象，保留keepFields字段
 */
class UrlArgsDatasetConvertor extends DatasetConvertor {

  /**
   * 保留字段
   */
  private String[] keepFields;

  public UrlArgsDatasetConvertor(Map<String, Object> params) {
    super(params);
    String fields = (String) params.get("keepFields");
    keepFields = fields.split(",");
  }

  @Override
  public Dataset convert(String fieldName, Dataset dataset) {
    Column args = callUDF("urlargs_split", col(fieldName));

    return dataset.withColumn(fieldName, callUDF("sub_element", args, lit(keepFields)));

  }
}

/**
 * base64转码器
 */
class Base64EncodeConvertor extends DatasetConvertor {

  public Base64EncodeConvertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset convert(String fieldName, Dataset dataset) {
    return dataset.withColumn(fieldName, base64(col(fieldName)));
  }
}


/**
 * base64解码器
 */
class Base64DecodeConvertor extends DatasetConvertor {

  public Base64DecodeConvertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset convert(String fieldName, Dataset dataset) {
    return dataset.withColumn(fieldName, unbase64(col(fieldName)));
  }
}


/**
 * md5转换器
 */
class MD5Convertor extends DatasetConvertor {

  public MD5Convertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset convert(String fieldName, Dataset dataset) {
    return dataset.withColumn(fieldName, md5(col(fieldName)));
  }
}