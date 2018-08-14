package com.weibo.dip.pipeline.processor.add;

import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.date_format;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/8/8
 */
public class CurrentDateStrDatasetAdder extends DatasetAddProcessor {

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
