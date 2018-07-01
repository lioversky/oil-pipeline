package com.weibo.dip.pipeline.processor.split;

import com.weibo.dip.pipeline.configuration.Configuration;
import java.util.List;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/1
 */
abstract class Spliter extends Configuration {

  public Spliter(Map<String, Object> parmas) {
    if (parmas != null) {
      addConfigs(parmas);
    }
  }

  public abstract Object split(Object value) throws Exception;
}

/**
 * 字符串拆分
 */
class StrSpliter extends Spliter {

  private String splitStr;

  public StrSpliter(Map<String, Object> parmas) {
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
 * List拆分
 */
class ListSpliter extends Spliter {

  public ListSpliter(Map<String, Object> parmas) {
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
class ArraySpliter extends Spliter {

  public ArraySpliter(Map<String, Object> parmas) {
    super(parmas);
  }

  @Override
  public Object split(Object value) throws Exception {
    return value;
  }
}