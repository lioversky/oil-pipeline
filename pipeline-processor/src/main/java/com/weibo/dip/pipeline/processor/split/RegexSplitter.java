package com.weibo.dip.pipeline.processor.split;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 正则拆分
 */
public class RegexSplitter extends FieldSplitProcessor {

  private Pattern pattern;
  private String regex;

  public RegexSplitter(Map<String, Object> params) {
    super(params);
    this.regex = (String) params.get("regex");
    if (Strings.isNullOrEmpty(regex)) {
      throw new AttrCanNotBeNullException("RegexSplitter regex can not be null!!!");
    }
    this.pattern = Pattern.compile(regex);
  }

  /**
   * 使用正则拆分成数组.
   *
   * @param value 拆分字段值
   * @return 正则匹配数组
   * @throws Exception 值非String时抛IllegalArgumentException
   */
  @Override
  public Object split(Object value) throws Exception {
    if (value instanceof String) {
      String line = (String) value;
      Matcher matcher = pattern.matcher(line);
      if (matcher.find()) {
        String[] result = new String[matcher.groupCount()];
        for (int i = 0; i < matcher.groupCount(); i++) {
          result[i] = matcher.group(i + 1);
        }
        return result;
      } else {
        return null;
      }

    } else {
      throw new IllegalArgumentException(String
          .format("Value type for regex split must be String ,but %s", value.getClass().getName()));
    }

  }
}
