package com.weibo.dip.pipeline.processor.replace;

import static org.apache.spark.sql.functions.base64;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.unbase64;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/19
 */
public abstract class DatasetReplacer extends Configuration {

  public DatasetReplacer(Map<String, Object> params) {

  }

  abstract Dataset replace(String fieldName, Dataset dataset);
}

/**
 * 正则替换
 */
class RegexReplacer extends DatasetReplacer {

  private String regex;
  private String target;

  @Override
  public Dataset replace(String fieldName, Dataset dataset) {
    return dataset.withColumn(fieldName, regexp_replace(col(fieldName), regex, target));
  }

  public RegexReplacer(Map<String, Object> params) {
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
