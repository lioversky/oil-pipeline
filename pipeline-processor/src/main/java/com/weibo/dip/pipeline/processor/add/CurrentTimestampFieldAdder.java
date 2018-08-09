package com.weibo.dip.pipeline.processor.add;

import java.util.Map;

/**
 * 增加当前时间戳
 */
public class CurrentTimestampFieldAdder extends FieldAddProcessor {

  public CurrentTimestampFieldAdder(Map<String, Object> params) {
    super(params);
  }

  @Override
  Object fieldAdd(Map<String, Object> data) {
    return System.currentTimeMillis();
  }
}
