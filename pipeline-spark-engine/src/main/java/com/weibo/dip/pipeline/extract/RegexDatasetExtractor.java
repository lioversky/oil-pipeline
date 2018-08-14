package com.weibo.dip.pipeline.extract;

import com.weibo.dip.pipeline.util.SparkUtil;
import java.util.ArrayList;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * 正则提取
 */
public class RegexDatasetExtractor extends DatasetExtractor {

  private String regex;
  private String[] targetFields;

  public RegexDatasetExtractor(Map<String, Object> params) {
    super(params);
    this.regex = (String) params.get("regex");
    targetFields = ((ArrayList<String>) params.get("columns")).toArray(new String[0]);
  }

  public Dataset extract(Dataset dataset) {
    return SparkUtil.regexSplitDatasetField(dataset, fieldName, fieldName, regex, targetFields);
  }
}
