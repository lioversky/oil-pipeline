package com.weibo.dip.pipeline.processor.replace;

/**
 * Create by hongxun on 2018/8/8
 */

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 正则转换
 */
public class RegexReplacer extends ReplaceProcessor {

  private String target;
  private Pattern pattern;

  public RegexReplacer(Map<String, Object> params) {
    super(params);
    String regex = (String) params.get("regex");
    if (Strings.isNullOrEmpty(regex)) {
      throw new AttrCanNotBeNullException("regex replace regex can not be null!!!");
    }
    this.target = Strings.nullToEmpty((String) params.get("target"));
    pattern = Pattern.compile(regex);
  }

  public Object replace(String data) throws Exception {
    Matcher m = pattern.matcher(data);
    if (m.find()) {
      return m.replaceAll(target);
    }
    return data;
  }
}
