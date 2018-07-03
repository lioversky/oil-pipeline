package com.weibo.dip.pipeline.processor.replace;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Create by hongxun on 2018/7/1
 */
abstract class Replacer extends Configuration {

  public Replacer(Map<String, Object> parmas) {
    if (parmas != null) {
      addConfigs(parmas);
    }
  }

  abstract Object replace(String value) throws Exception;
}

/**
 * 字符串转日期
 */

class StrToDateReplacer extends Replacer {

  /**
   * 包括 yyyyMMdd等格式
   */
  private DateTimeFormatter dateTimeFormat;

  public StrToDateReplacer(Map<String, Object> parmas) {
    super(parmas);
    String source = (String) parmas.get("source");
    if (Strings.isNullOrEmpty(source)) {
      throw new AttrCanNotBeNullException(
          "datestr replace to date sourceFormat can not be null!!!");
    }
    dateTimeFormat = DateTimeFormat.forPattern(source);
  }

  public Date replace(String data) throws Exception {
    return DateTime.parse(data, dateTimeFormat).toDate();

  }
}

/**
 * 字符串转时间戳
 */
class StrToTimestampReplacer extends Replacer {

  /**
   * 包括 yyyyMMdd等格式
   */
  private DateTimeFormatter dateTimeFormat;

  public StrToTimestampReplacer(Map<String, Object> parmas) {
    super(parmas);
    String source = (String) parmas.get("source");
    if (Strings.isNullOrEmpty(source)) {
      throw new AttrCanNotBeNullException(
          "datestr replace to timestamp sourceFormat can not be null!!!");
    }
    dateTimeFormat = DateTimeFormat.forPattern(source);
  }

  public Long replace(String data) throws Exception {
    return DateTime.parse(data, dateTimeFormat).getMillis();
  }
}

/**
 * 字符串转unix时间戳，秒数
 */
class StrToUnixTimestampReplacer extends Replacer {

  private DateTimeFormatter dateTimeFormat;

  public StrToUnixTimestampReplacer(Map<String, Object> parmas) {
    super(parmas);
    String source = (String) parmas.get("source");
    if (Strings.isNullOrEmpty(source)) {
      throw new AttrCanNotBeNullException(
          "datestr replace to unix timestamp sourceFormat can not be null!!!");
    }
    dateTimeFormat = DateTimeFormat.forPattern(source);
  }

  public Long replace(String data) throws Exception {
    return DateTime.parse(data, dateTimeFormat).getMillis() / 1000;

  }
}

/**
 * 字符串转字符串
 */
class StrToDateStrReplacer extends Replacer {

  /**
   * 包括 yyyyMMdd等格式
   */
  private DateTimeFormatter sourceFormat;
  /**
   * 包括 yyyyMMdd等格式
   */
  private DateTimeFormatter targetFormat;

  public StrToDateStrReplacer(Map<String, Object> parmas) {
    super(parmas);
    String source = (String) parmas.get("source");
    String target = (String) parmas.get("target");
    if (Strings.isNullOrEmpty(source)) {
      throw new AttrCanNotBeNullException(
          "datestr replace to datestr sourceFormat can not be null!!!");
    }
    if (Strings.isNullOrEmpty(target)) {
      throw new AttrCanNotBeNullException(
          "datestr replace to datestr targetFormat can not be null!!!");
    }
    sourceFormat = DateTimeFormat.forPattern(source);
    targetFormat = DateTimeFormat.forPattern(target);
  }

  public Object replace(String data) throws Exception {
    return DateTime.parse(data, sourceFormat).toString(targetFormat);
  }
}


/**
 * unix数字转DateStr
 */
class UnixToDateStrReplacer extends Replacer {

  private DateTimeFormatter dateTimeFormat;

  public UnixToDateStrReplacer(Map<String, Object> parmas) {
    super(parmas);
    String target = (String) parmas.get("target");
    if (Strings.isNullOrEmpty(target)) {
      throw new AttrCanNotBeNullException(
          "unix timestamp replace to datestr targetFormat can not be null!!!");
    }
    dateTimeFormat = DateTimeFormat.forPattern(target);
  }

  public String replace(String data) throws Exception {
    return new DateTime(Long.parseLong(data) * 1000).toString(dateTimeFormat);
  }
}


class TimestampToDateStrReplacer extends Replacer {

  private DateTimeFormatter dateTimeFormat;

  public TimestampToDateStrReplacer(Map<String, Object> parmas) {
    super(parmas);
    String target = (String) parmas.get("target");
    if (Strings.isNullOrEmpty(target)) {
      throw new AttrCanNotBeNullException(
          "timestamp replace to datestr targetFormat can not be null!!!");
    }
    dateTimeFormat = DateTimeFormat.forPattern(target);
  }

  @Override
  Object replace(String value) throws Exception {
    Long time = Long.parseLong(value);
    return new DateTime(time).toString(dateTimeFormat);
  }
}

/**
 * 替换,接收原字符和目标字符,进行替换
 */
class ReplaceStrReplacer extends Replacer {

  private String source;
  private String target;

  public ReplaceStrReplacer(Map<String, Object> parmas) {
    super(parmas);
    this.source = (String) parmas.get("source");
    if (Strings.isNullOrEmpty(source)) {
      throw new AttrCanNotBeNullException("str replace source can not be null!!!");
    }
    this.target = Strings.nullToEmpty((String) parmas.get("target"));
    if (target == null) {
      throw new AttrCanNotBeNullException("str replace target can not be null!!!");
    }
  }

  public Object replace(String data) throws Exception {
    return data.replaceAll(source, target);
  }
}

/**
 * 正则转换
 */
class RegexReplacer extends Replacer {

  private String target;
  private Pattern p;

  public RegexReplacer(Map<String, Object> parmas) {
    super(parmas);
    String regex = (String) parmas.get("regex");
    if (Strings.isNullOrEmpty(regex)) {
      throw new AttrCanNotBeNullException("regex replace regex can not be null!!!");
    }
    this.target = Strings.nullToEmpty((String) parmas.get("target"));
    p = Pattern.compile(regex);
  }

  public Object replace(String data) throws Exception {
    Matcher m = p.matcher(data);
    if (m.find()) {
      return m.replaceAll(target);
    }
    return data;
  }
}
