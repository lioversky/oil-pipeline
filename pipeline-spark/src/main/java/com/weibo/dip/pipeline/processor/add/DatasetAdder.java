package com.weibo.dip.pipeline.processor.add;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.lit;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/23
 */
public abstract class DatasetAdder {

  protected String targetField;

  public DatasetAdder(Map<String, Object> params) {
    targetField = (String) params.get("targetField");
  }

  public abstract Dataset add(Dataset dataset);
}

class CopyDatasetAdder extends DatasetAdder {

  private String sourceField;


  public CopyDatasetAdder(Map<String, Object> params) {
    super(params);
    sourceField = (String) params.get("sourceField");
  }

  @Override
  public Dataset add(Dataset dataset) {
    return dataset.withColumn(targetField, col(sourceField));
  }
}

class CurrentDateStrDatasetAdder extends DatasetAdder {

  private String dateFormat;

  public CurrentDateStrDatasetAdder(Map<String, Object> params) {
    super(params);
    dateFormat = (String) params.get("dateFormat");
  }

  @Override
  public Dataset add(Dataset dataset) {
    return dataset.withColumn(targetField, date_format(current_timestamp(), dateFormat));
  }
}


/**
 * 增加当前时间戳
 */
class CurrentTimestampDatasetAdder extends DatasetAdder {


  public CurrentTimestampDatasetAdder(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset add(Dataset dataset) {
    return dataset.withColumn(targetField, current_timestamp());
  }
}

/**
 * 增加当前unix时间戳
 */
class CurrentUnixTimestampDatasetAdder extends DatasetAdder {

  public CurrentUnixTimestampDatasetAdder(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset add(Dataset dataset) {
    return dataset.withColumn(targetField, unix_timestamp());
  }
}

/**
 * 增加固定值
 */
class FixedValueDatasetAdder extends DatasetAdder {

  private Object fixedValue;

  public FixedValueDatasetAdder(Map<String, Object> params) {
    super(params);
    fixedValue = params.get("fixedValue");
  }

  @Override
  public Dataset add(Dataset dataset) {
    return dataset.withColumn(targetField, lit(fixedValue));
  }
}