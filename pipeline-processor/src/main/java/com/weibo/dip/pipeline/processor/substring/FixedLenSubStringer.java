package com.weibo.dip.pipeline.processor.substring;

import java.util.Map;

/**
 * Create by hongxun on 2018/8/8
 */
public class FixedLenSubStringer extends SubStringProcessor {

  private int start;
  private int length;

  @Override
  String substring(String value) throws Exception {
    int end = value.length() > start + length ? start + length : value.length();
    return value.substring(start, end);
  }

  public FixedLenSubStringer(Map<String, Object> params) {
    super(params);
    this.start = params.containsKey("start") ? ((Number) params.get("start")).intValue() : 0;
    this.length = params.containsKey("length") ? ((Number) params.get("length")).intValue() : -1;
  }
}
