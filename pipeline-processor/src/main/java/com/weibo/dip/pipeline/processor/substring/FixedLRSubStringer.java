package com.weibo.dip.pipeline.processor.substring;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * 定长截取.
 */
public class FixedLRSubStringer extends SubStringProcessor {

  private int left;
  private int right;

  public FixedLRSubStringer(Map<String, Object> params) {
    super(params);
    this.left = params.containsKey("left") ? ((Number) params.get("left")).intValue() : 0;
    this.right = params.containsKey("right") ? ((Number) params.get("right")).intValue() : -1;
  }

  @Override
  String substring(String value) throws Exception {
    int endIndex = right < 0 ? value.length() : value.length() - right;
    return StringUtils.substring(value,left,endIndex);
  }
}
