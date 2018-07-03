package com.weibo.dip.pipeline.processor.add;

import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;
import org.joda.time.DateTime;
import org.nutz.lang.Strings;

/**
 * Create by hongxun on 2018/7/1
 */
abstract class FieldAdder extends Configuration {

  public FieldAdder(Map<String, Object> parmas) {
    if (parmas != null) {
      addConfigs(parmas);
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

  public CopyFieldAdder(Map<String, Object> parmas) {
    super(parmas);
    sourceField = (String) parmas.get("sourceField");
    if(Strings.isBlank(sourceField)){
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

  public CurrentDateStrFieldAdder(Map<String, Object> parmas) {
    super(parmas);
    dateFormat = (String) parmas.get("dateFormat");
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

  public CurrentTimestampFieldAdder(Map<String, Object> parmas) {
    super(parmas);
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

  public CurrentUnixTimestampFieldAdder(Map<String, Object> parmas) {
    super(parmas);
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

  public FixedValueFieldAdder(Map<String, Object> parmas) {
    super(parmas);
    fixedValue = parmas.get("fixedValue");
  }

  @Override
  Object fieldAdd(Map<String, Object> data) {
    return fixedValue;
  }
}