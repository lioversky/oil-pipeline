package com.weibo.dip.pipeline.processor.substring;

import com.weibo.dip.pipeline.processor.FieldDatasetProcessor;
import java.util.Map;
import org.apache.spark.sql.Dataset;

import static org.apache.spark.sql.functions.regexp_extract;
import static org.apache.spark.sql.functions.col;

/**
 * Create by hongxun on 2018/8/21
 */
public class RegexDatasetSubStringer extends FieldDatasetProcessor {

  private String regex;

  public RegexDatasetSubStringer(Map<String, Object> params) {
    super(params);
    regex = (String) params.get("regex");
  }

  @Override
  protected Dataset fieldProcess(Dataset dataset) {
    return dataset.withColumn(fieldName, regexp_extract(col(fieldName), regex, 1));
  }
}
