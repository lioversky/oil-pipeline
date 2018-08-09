package com.weibo.dip.pipeline.processor.replace;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * 字符串转unix时间戳，秒数
 */
public class StrToUnixTimestampReplacer extends ReplaceProcessor {

  private DateTimeFormatter dateTimeFormat;

  public StrToUnixTimestampReplacer(Map<String, Object> params) {
    super(params);
    String source = (String) params.get("source");
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
