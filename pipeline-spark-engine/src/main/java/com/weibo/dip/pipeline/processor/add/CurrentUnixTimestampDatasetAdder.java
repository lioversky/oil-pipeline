package com.weibo.dip.pipeline.processor.add;

import static org.apache.spark.sql.functions.unix_timestamp;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * 增加当前unix时间戳
 */
public class CurrentUnixTimestampDatasetAdder extends DatasetAddProcessor {

  public CurrentUnixTimestampDatasetAdder(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset add(Dataset dataset) {
    return dataset.withColumn(targetField, unix_timestamp());
  }
}
