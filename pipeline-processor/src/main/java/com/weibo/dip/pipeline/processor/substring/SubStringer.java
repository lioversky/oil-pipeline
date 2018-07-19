package com.weibo.dip.pipeline.processor.substring;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

/**
 * Create by hongxun on 2018/7/1
 */
abstract class SubStringer extends Configuration {

  public SubStringer(Map<String, Object> params) {
    if (params != null) {
      addConfigs(params);
    }
  }

  abstract String substring(String value) throws Exception;
}

/**
 * 去空格.
 */
class TrimSubStringer extends SubStringer {

  public TrimSubStringer(Map<String, Object> params) {
    super(params);
  }

  @Override
  String substring(String value) throws Exception {
    return value.trim();
  }
}

/**
 * 定长截取.
 */
class FixedLRSubStringer extends SubStringer {

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

class FixedLenSubStringer extends SubStringer {

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

/**
 * 匹配截取.
 */
class MatchSubStringer extends SubStringer {

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

class RegexExtractSubStringer extends SubStringer {

  @Override
  String substring(String value) throws Exception {
    Matcher m = pattern.matcher(value);
    if (m.find()) {
      return m.group();
    }
    return defaultValue;
  }

  private String regex;
  private String defaultValue;
  private Pattern pattern;

  public RegexExtractSubStringer(Map<String, Object> params) {
    super(params);
    regex = (String) params.get("regex");
    if (Strings.isNullOrEmpty(regex)) {
      throw new AttrCanNotBeNullException(
          "RegexExtract regex can not be null!!!");
    }
    pattern = Pattern.compile(regex);
    defaultValue = (String) params.get("defaultValue");
  }
}
