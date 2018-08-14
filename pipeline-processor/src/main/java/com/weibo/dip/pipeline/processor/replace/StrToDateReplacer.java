package com.weibo.dip.pipeline.processor.replace;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Date;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * 字符串转日期
 */

public class StrToDateReplacer extends ReplaceProcessor {

  /**
   * 包括 yyyyMMdd等格式
   */
  private DateTimeFormatter dateTimeFormat;

  /**
   * 构造函数
   *
   * @param params 参数
   */
  public StrToDateReplacer(Map<String, Object> params) {
    super(params);
    String source = (String) params.get("source");
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
