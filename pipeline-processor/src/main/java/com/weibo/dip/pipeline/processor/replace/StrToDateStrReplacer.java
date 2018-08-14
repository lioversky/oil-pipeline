package com.weibo.dip.pipeline.processor.replace;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * 字符串转字符串
 */
public class StrToDateStrReplacer extends ReplaceProcessor {

  /**
   * 包括 yyyyMMdd等格式
   */
  private DateTimeFormatter sourceFormat;
  /**
   * 包括 yyyyMMdd等格式
   */
  private DateTimeFormatter targetFormat;

  /**
   * 构造函数
   *
   * @param params 参数
   */
  public StrToDateStrReplacer(Map<String, Object> params) {
    super(params);
    String source = (String) params.get("source");
    String target = (String) params.get("target");
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
