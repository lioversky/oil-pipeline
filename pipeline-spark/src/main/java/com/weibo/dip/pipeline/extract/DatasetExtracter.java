package com.weibo.dip.pipeline.extract;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_extract;
import static org.apache.spark.sql.functions.split;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/19
 */
public abstract class DatasetExtracter {

  protected String fieldName;

  public DatasetExtracter(Map<String, Object> params) {
    fieldName = (String) params.get("fieldName");
  }

  public abstract Dataset extract(Dataset dataset);
}

/**
 * 分隔提取
 */
class SplitExtracter extends DatasetExtracter {

  private boolean keepField;
  private String splitStr;
  private String[] targetFields;

  @Override
  public Dataset extract(Dataset dataset) {
    String arrName = keepField ? "_array_" + fieldName + "_" : fieldName;
    dataset = dataset.withColumn(arrName, split(col(fieldName), splitStr));
    for (int i = 0; i < targetFields.length; i++) {
      dataset = dataset.withColumn(targetFields[i], col(arrName).getItem(i));
    }
    return dataset.drop(arrName);
  }

  public SplitExtracter(Map<String, Object> params) {
    super(params);
    splitStr = (String) params.get("splitStr");
    String targetField = (String) params.get("targetFields");
    targetFields = StringUtils.split(targetField, ",");
  }
}


/**
 * 正则提取
 */
class RegexpExtracter extends DatasetExtracter {

  private String regex;
  private Integer groupIdx;


  public RegexpExtracter(Map<String, Object> params) {
    super(params);
    this.regex = (String) params.get("regex");
    this.groupIdx = (Integer) params.get("groupIdx");
  }

  public Dataset extract(Dataset dataset) {
    return dataset.withColumn(fieldName, regexp_extract(col(fieldName), regex, groupIdx));
  }
}

/**
 * json提取
 */
class JsonExtracter extends DatasetExtracter {

  public JsonExtracter(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset extract(Dataset dataset) {
    return null;
  }
}