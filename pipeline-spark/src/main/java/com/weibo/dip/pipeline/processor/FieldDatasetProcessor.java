package com.weibo.dip.pipeline.processor;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;

import static org.apache.spark.sql.functions.*;

/**
 * Create by hongxun on 2018/7/10
 */
public abstract class FieldDatasetProcessor extends DatasetProcessor {

  protected String fieldName;

  public FieldDatasetProcessor(Map<String, Object> params) {
    super(params);
    fieldName = (String) params.get("fieldName");
  }

  @Override
  public Dataset process(Dataset data) throws Exception {
    return fieldProcess(data);
  }

  protected abstract Dataset fieldProcess(Dataset dataset);
}

