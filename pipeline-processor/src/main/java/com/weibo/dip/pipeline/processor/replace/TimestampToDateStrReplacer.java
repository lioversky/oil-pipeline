package com.weibo.dip.pipeline.processor.replace;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Create by hongxun on 2018/8/8
 */
public class TimestampToDateStrReplacer extends ReplaceProcessor {

  private DateTimeFormatter dateTimeFormat;

  public TimestampToDateStrReplacer(Map<String, Object> params) {
    super(params);
    String target = (String) params.get("target");
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
