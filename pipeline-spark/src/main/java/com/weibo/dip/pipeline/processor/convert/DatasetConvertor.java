package com.weibo.dip.pipeline.processor.convert;


import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import com.weibo.dip.pipeline.configuration.Configuration;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/23
 */
public abstract class DatasetConvertor extends Configuration {

  public DatasetConvertor(Map<String, Object> params) {
  }

  public abstract Dataset convert(String fieldName, Dataset dataset);
}

class UrlArgsDatasetConvertor extends DatasetConvertor {

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
