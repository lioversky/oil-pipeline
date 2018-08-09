package com.weibo.dip.pipeline.processor.add;

import java.util.Map;

/**
 * 增加当前unix时间戳
 */
public class CurrentUnixTimestampFieldAdder extends FieldAddProcessor {

  public CurrentUnixTimestampFieldAdder(Map<String, Object> params) {
    super(params);
  }

  @Override
  Object fieldAdd(Map<String, Object> data) {
    return System.currentTimeMillis() / 1000;
  }
}
