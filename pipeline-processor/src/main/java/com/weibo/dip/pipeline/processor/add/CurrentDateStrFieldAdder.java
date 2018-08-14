package com.weibo.dip.pipeline.processor.add;

import java.util.Map;
import org.joda.time.DateTime;

/**
 * 增加当前时间字符串
 */
public class CurrentDateStrFieldAdder extends FieldAddProcessor {

  private String dateFormat;

  public CurrentDateStrFieldAdder(Map<String, Object> params) {
    super(params);
    dateFormat = (String) params.get("dateFormat");
  }


  @Override
  Object fieldAdd(Map<String, Object> data) {
    return new DateTime().toString(dateFormat);
  }
}
