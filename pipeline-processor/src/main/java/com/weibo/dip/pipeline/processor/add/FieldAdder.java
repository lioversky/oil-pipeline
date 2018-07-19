package com.weibo.dip.pipeline.processor.add;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;
import org.joda.time.DateTime;

/**
 * Create by hongxun on 2018/7/1
 */
abstract class FieldAdder extends Configuration {

  public FieldAdder(Map<String, Object> params) {
    if (params != null) {
      addConfigs(params);
    }
  }

  abstract Object fieldAdd(Map<String, Object> data);
}

/**
 * 复制
 */
class CopyFieldAdder extends FieldAdder {

  /**
   * 复制的源名称
   */
  private String sourceField;

  public CopyFieldAdder(Map<String, Object> params) {
    super(params);
    sourceField = (String) params.get("sourceField");
    if (Strings.isNullOrEmpty(sourceField)) {
      throw new AttrCanNotBeNullException("Fieldcopy sourceField can not be null!!!");
    }
  }

  @Override
  Object fieldAdd(Map<String, Object> data) {
    return data.get(sourceField);
  }
}

/**
 * 增加当前时间字符串
 */
class CurrentDateStrFieldAdder extends FieldAdder {

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

/**
 * 增加当前时间戳
 */
class CurrentTimestampFieldAdder extends FieldAdder {

  public CurrentTimestampFieldAdder(Map<String, Object> params) {
    super(params);
  }

  @Override
  Object fieldAdd(Map<String, Object> data) {
    return System.currentTimeMillis();
  }
}

/**
 * 增加当前unix时间戳
 */
class CurrentUnixTimestampFieldAdder extends FieldAdder {

  public CurrentUnixTimestampFieldAdder(Map<String, Object> params) {
    super(params);
  }

  @Override
  Object fieldAdd(Map<String, Object> data) {
    return System.currentTimeMillis() / 1000;
  }
}

/**
 * 增加固定值
 */
class FixedValueFieldAdder extends FieldAdder {

  private Object fixedValue;

  public FixedValueFieldAdder(Map<String, Object> params) {
    super(params);
    fixedValue = params.get("fixedValue");
  }

  @Override
  Object fieldAdd(Map<String, Object> data) {
    return fixedValue;
  }
}