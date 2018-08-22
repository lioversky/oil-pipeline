package com.weibo.dip.pipeline.processor.substring;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Create by hongxun on 2018/8/8
 */
public class RegexExtractSubStringer extends SubStringProcessor {

  @Override
  String substring(String value) throws Exception {
    Matcher m = pattern.matcher(value);
    if (m.find()) {
      return m.group(1);
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
