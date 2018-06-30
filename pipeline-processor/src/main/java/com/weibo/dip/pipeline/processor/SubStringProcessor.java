package com.weibo.dip.pipeline.processor;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.configuration.Configuration;
import java.util.Map;

/**
 * 列截取处理器.
 * Create by hongxun on 2018/6/27
 */
public class SubStringProcessor extends FieldProcessor {

  private SubStringer subStringer;

  public SubStringProcessor(boolean fieldNotExistError, String columnName,
      SubStringer subStringer) {
    super(fieldNotExistError, columnName);
    this.subStringer = subStringer;
  }

  public SubStringProcessor(boolean fieldNotExistError, String columnName) {
    super(fieldNotExistError, columnName);
  }

  @Override
  public Object columnProcess(Object data) throws Exception {
    return subStringer.subString((String) data);
  }

}

abstract class SubStringer extends Configuration {

  public SubStringer(Map<String, Object> parmas) {
    if (parmas != null) {
      addConfigs(parmas);
    }
  }

  abstract String subString(String value) throws Exception;
}

/**
 * 去空格.
 */
class TrimSubStringer extends SubStringer {

  public TrimSubStringer(Map<String, Object> parmas) {
    super(parmas);
  }

  @Override
  String subString(String value) throws Exception {
    return value.trim();
  }
}

/**
 * 定长截取.
 */
class FixedSubStringer extends SubStringer {

  private int begin;
  private int end;

  public FixedSubStringer(Map<String, Object> parmas) {
    super(parmas);
    this.begin = parmas.containsKey("begin") ? ((Number) parmas.get("begin")).intValue() : 0;
    this.end = parmas.containsKey("end") ? ((Number) parmas.get("end")).intValue() : -1;
  }

  @Override
  String subString(String value) throws Exception {
    int endIndex = end < 0 ? value.length() : value.length() - end;
    if (endIndex < 0) {
      throw new IllegalArgumentException(
          String.format("end value: %d is bigger than value length: %d", end, value.length()));
    }
    if (endIndex > begin) {
      return value.substring(begin, endIndex);
    } else {
      return value.substring(begin);
    }
  }
}

/**
 * 匹配截取.
 */
class MatchSubStringer extends SubStringer {

  private String beginStr;
  private String endStr;

  public MatchSubStringer(Map<String, Object> parmas) {
    super(parmas);
    this.beginStr = (String) parmas.get("beginStr");
    this.endStr = (String) parmas.get("endStr");
  }

  @Override
  String subString(String value) throws Exception {
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

