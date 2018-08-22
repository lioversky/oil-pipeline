package com.weibo.dip.pipeline.processor.convert;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;

/**
 * url参数转换成对象，保留keepFields字段
 */
public class ExtendArgsDatasetConvertor extends DatasetConvertProcessor {

  /**
   * 保留字段
   */
  private String[] keepFields;

  public ExtendArgsDatasetConvertor(Map<String, Object> params) {
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
