package com.weibo.dip.pipeline.processor.flatten;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;

/**
 * 展开当前字段
 */
class FieldFlattener extends FlattenProcessor {

  private String fieldName;

  public FieldFlattener(Map<String, Object> params) {
    super(params);
    fieldName = (String) params.get("fieldName");
    if (Strings.isNullOrEmpty(fieldName)) {
      throw new AttrCanNotBeNullException("FieldFlattener fieldName can not be null!!!");
    }
  }

  @Override
  public Map<String, Object> flatten(Map<String, Object> data) throws Exception {
    return flattenMap(fieldName, data.get(fieldName), 1);
  }
}
