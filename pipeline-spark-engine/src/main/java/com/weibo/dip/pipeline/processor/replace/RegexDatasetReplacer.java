package com.weibo.dip.pipeline.processor.replace;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * 正则替换
 */
public class RegexDatasetReplacer extends DatasetReplaceProcessor {

  private String regex;
  private String target;

  @Override
  public Dataset replace(String fieldName, Dataset dataset) {
    return dataset.withColumn(fieldName, regexp_replace(col(fieldName), regex, target));
  }

  public RegexDatasetReplacer(Map<String, Object> params) {
    super(params);
    this.regex = (String) params.get("regex");
    if (Strings.isNullOrEmpty(regex)) {
      throw new AttrCanNotBeNullException("str replace regex can not be null!!!");
    }
    this.target = Strings.nullToEmpty((String) params.get("target"));
    if (target == null) {
      throw new AttrCanNotBeNullException("str replace target can not be null!!!");
    }
  }
}
