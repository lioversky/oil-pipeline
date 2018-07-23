package com.weibo.dip.pipeline.processor.split;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.pipeline.util.DatasetUtil;
import java.util.ArrayList;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;

/**
 * Create by hongxun on 2018/7/23
 */
public abstract class DatasetSpliter extends Configuration {

  public DatasetSpliter(Map<String, Object> params) {
  }

  public abstract Dataset split(String fieldName, Dataset dataset);
}

class DelimiterDatasetSplitter extends DatasetSpliter {

  private String splitStr;
  private String[] targetFields;

  public DelimiterDatasetSplitter(Map<String, Object> params) {
    super(params);
    splitStr = (String) params.get("splitStr");
    String fields = (String) params.get("targetFields");
    targetFields = fields.split(",");
  }

  @Override
  public Dataset split(String fieldName, Dataset dataset) {
    return DatasetUtil.splitDataset(dataset, fieldName, fieldName, splitStr, targetFields);
  }
}

class JsonDatasetSplitter extends DatasetSpliter {

  public JsonDatasetSplitter(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset split(String fieldName, Dataset dataset) {
    return null;
  }
}

class RegexDatasetSplitter extends DatasetSpliter {

  public RegexDatasetSplitter(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset split(String fieldName, Dataset dataset) {
    return null;
  }
}