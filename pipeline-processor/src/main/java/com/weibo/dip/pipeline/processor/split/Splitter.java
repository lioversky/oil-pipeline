package com.weibo.dip.pipeline.processor.split;

import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.util.GsonUtil;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 字段拆分.
 * Create by hongxun on 2018/7/1
 */
abstract class Splitter extends Configuration {

  public Splitter(Map<String, Object> parmas) {
    if (parmas != null) {
      addConfigs(parmas);
    }
  }

  /**
   * 用于各类实现的拆分方法，返回数组.
   *
   * @param value 拆分字段值
   * @return 数组
   * @throws Exception 异常
   */
  public abstract Object split(Object value) throws Exception;
}

/**
 * 字符串拆分
 */
class DelimiterSplitter extends Splitter {

  private String splitStr;

  public DelimiterSplitter(Map<String, Object> parmas) {
    super(parmas);
    splitStr = (String) parmas.get("splitStr");
  }

  @Override
  public Object split(Object value) throws Exception {
    //    return StringUtils.split((String)value,splitStr);
    return ((String) value).split(splitStr);
  }
}

/**
 * 正则拆分
 */
class RegexSplitter extends Splitter {

  private Pattern pattern;
  private String regex;

  public RegexSplitter(Map<String, Object> parmas) {
    super(parmas);
    this.regex = (String) parmas.get("regex");
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

/**
 * json拆分
 */
class JsonSplitter extends Splitter {

  /**
   * json字符串转成Object对象，不指定是数组或是map
   *
   * @param value 拆分字段值
   * @return Object
   * @throws Exception IllegalArgumentException和json解析异常
   */
  @Override
  public Object split(Object value) throws Exception {
    if (value instanceof String) {
      String line = (String) value;
      return GsonUtil.fromJson(line, Object.class);
    } else {
      throw new IllegalArgumentException(String
          .format("Value type for regex split must be String ,but %s", value.getClass().getName()));
    }
  }

  public JsonSplitter(Map<String, Object> parmas) {
    super(parmas);
  }
}

/**
 * List拆分
 */
class ListSplitter extends Splitter {

  public ListSplitter(Map<String, Object> parmas) {
    super(parmas);
  }

  @Override
  public Object split(Object value) throws Exception {
    List list = (List) value;
    return list.toArray(new Object[list.size()]);
  }
}

/**
 * Array拆分
 */
class ArraySplitter extends Splitter {

  public ArraySplitter(Map<String, Object> parmas) {
    super(parmas);
  }

  @Override
  public Object split(Object value) throws Exception {
    return value;
  }
}