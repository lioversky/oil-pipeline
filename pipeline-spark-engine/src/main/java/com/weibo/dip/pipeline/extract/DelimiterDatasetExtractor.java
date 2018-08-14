package com.weibo.dip.pipeline.extract;

import com.weibo.dip.pipeline.util.SparkUtil;
import java.util.ArrayList;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * 分隔提取
 */
public class DelimiterDatasetExtractor extends DatasetExtractor {

  private boolean keepField = false;
  private String splitStr;
  private String[] targetFields;

  @Override
  public Dataset extract(Dataset dataset) {
    return SparkUtil.splitDatasetField(dataset, fieldName, fieldName, splitStr, targetFields);
  }

  public DelimiterDatasetExtractor(Map<String, Object> params) {
    super(params);
    splitStr = (String) params.get("split");
    targetFields = ((ArrayList<String>) params.get("columns")).toArray(new String[0]);
  }
}
