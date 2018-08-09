package com.weibo.dip.pipeline.processor.replace;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * unix数字转DateStr
 */
public class UnixToDateStrReplacer extends ReplaceProcessor {

  private DateTimeFormatter dateTimeFormat;

  public UnixToDateStrReplacer(Map<String, Object> params) {
    super(params);
    String target = (String) params.get("target");
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
