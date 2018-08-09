package com.weibo.dip.pipeline.processor.add;

import static org.apache.spark.sql.functions.lit;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * 增加固定值
 */
public class FixedValueDatasetAdder extends DatasetAddProcessor {

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
