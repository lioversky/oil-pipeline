package com.weibo.dip.pipeline.processor.add;

import static org.apache.spark.sql.functions.current_timestamp;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * 增加当前时间戳
 */
public class CurrentTimestampDatasetAdder extends DatasetAddProcessor {


  public CurrentTimestampDatasetAdder(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset add(Dataset dataset) {
    return dataset.withColumn(targetField, current_timestamp());
  }
}
