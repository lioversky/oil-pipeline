package com.weibo.dip.pipeline.processor.substring;

import java.util.Map;

/**
 * 去空格.
 */
public class TrimSubStringer extends SubStringProcessor {

  public TrimSubStringer(Map<String, Object> params) {
    super(params);
  }

  @Override
  String substring(String value) throws Exception {
    return value.trim();
  }
}
