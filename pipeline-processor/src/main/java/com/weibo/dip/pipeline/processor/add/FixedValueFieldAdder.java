package com.weibo.dip.pipeline.processor.add;

import java.util.Map;

/**
 * 增加固定值
 */
public class FixedValueFieldAdder extends FieldAddProcessor {

  private Object fixedValue;

  public FixedValueFieldAdder(Map<String, Object> params) {
    super(params);
    fixedValue = params.get("fixedValue");
  }

  @Override
  Object fieldAdd(Map<String, Object> data) {
    return fixedValue;
  }
}
