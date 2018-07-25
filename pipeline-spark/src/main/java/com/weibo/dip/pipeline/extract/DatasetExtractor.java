package com.weibo.dip.pipeline.extract;

import com.weibo.dip.pipeline.util.DatasetUtil;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * dataset提取器
 * Create by hongxun on 2018/7/19
 */
public abstract class DatasetExtractor implements Serializable {

  protected String fieldName = "_value_";

  public DatasetExtractor(Map<String, Object> params) {
    if (params.containsKey("fieldName")) {
      fieldName = (String) params.get("fieldName");
    }
  }

  public abstract Dataset extract(Dataset dataset);
}

/**
 * 分隔提取
 */
class DelimiterDatasetExtractor extends DatasetExtractor {

  private boolean keepField = false;
  private String splitStr;
  private String[] targetFields;

  @Override
  public Dataset extract(Dataset dataset) {
    return DatasetUtil.splitDatasetField(dataset, fieldName, fieldName, splitStr, targetFields);
  }

  public DelimiterDatasetExtractor(Map<String, Object> params) {
    super(params);
    splitStr = (String) params.get("split");
    targetFields = ((ArrayList<String>) params.get("columns")).toArray(new String[0]);
  }
}


/**
 * 正则提取
 */
class RegexDatasetExtractor extends DatasetExtractor {

  private String regex;
  private String[] targetFields;

  public RegexDatasetExtractor(Map<String, Object> params) {
    super(params);
    this.regex = (String) params.get("regex");
    targetFields = ((ArrayList<String>) params.get("columns")).toArray(new String[0]);
  }

  public Dataset extract(Dataset dataset) {
    return DatasetUtil.regexSplitDatasetField(dataset, fieldName, fieldName, regex, targetFields);
  }
}


/**
 * json提取
 */
class JsonDatasetExtractor extends DatasetExtractor {

  public JsonDatasetExtractor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset extract(Dataset dataset) {
    return null;
  }
}

class OrderMultipleDatasetExtractor extends DatasetExtractor {

  public OrderMultipleDatasetExtractor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset extract(Dataset dataset) {
    return null;
  }
}