package com.weibo.dip.pipeline.processor.substring;

import com.google.common.base.Strings;
import java.util.Map;

/**
 * 匹配截取.
 */
public class MatchSubStringer extends SubStringProcessor {

  private String beginStr;
  private String endStr;

  public MatchSubStringer(Map<String, Object> params) {
    super(params);
    this.beginStr = (String) params.get("beginStr");
    this.endStr = (String) params.get("endStr");
  }

  @Override
  String substring(String value) throws Exception {
    //    todo: 复杂条件
    int beginIndex = Strings.isNullOrEmpty(beginStr) ? 0 : value.indexOf(beginStr);

    if (beginIndex >= 0) {
      beginIndex += beginStr.length();
    } else {
      beginIndex = 0;
    }
    int endIndex = Strings.isNullOrEmpty(endStr) ? -1 : value.lastIndexOf(endStr);
    if (endIndex < 0) {
      endIndex = value.length();
    }
    if (endIndex > beginIndex) {
      return value.substring(beginIndex, endIndex);
    } else {
      return null;
    }
  }
}
