package com.weibo.dip.pipeline.processor.split;

import com.weibo.dip.pipeline.util.DatasetUtil;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/8/8
 */
public class DelimiterDatasetSplitter extends DatasetSplitProcessor {

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
    return DatasetUtil.splitDatasetField(dataset, fieldName, fieldName, splitStr, targetFields);
  }
}
