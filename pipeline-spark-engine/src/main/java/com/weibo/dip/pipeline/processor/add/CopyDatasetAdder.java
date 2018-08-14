package com.weibo.dip.pipeline.processor.add;

import static org.apache.spark.sql.functions.col;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/8/8
 */
public class CopyDatasetAdder extends DatasetAddProcessor {

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
