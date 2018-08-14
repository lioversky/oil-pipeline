package com.weibo.dip.pipeline.processor.add;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;

/**
 * 复制
 */
public class CopyFieldAdder extends FieldAddProcessor {

  /**
   * 复制的源名称
   */
  private String sourceField;

  public CopyFieldAdder(Map<String, Object> params) {
    super(params);
    sourceField = (String) params.get("sourceField");
    if (Strings.isNullOrEmpty(sourceField)) {
      throw new AttrCanNotBeNullException("Fieldcopy sourceField can not be null !!!");
    }
  }

  @Override
  Object fieldAdd(Map<String, Object> data) {
    return data.get(sourceField);
  }
}
