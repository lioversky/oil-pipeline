package com.weibo.dip.pipeline.processor;

import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.pipeline.exception.FieldExistException;
import java.util.Map;
import org.joda.time.DateTime;

/**
 * 增加新列.
 * 包括：复制列
 * 增加固定值
 * 增加各类型时间
 */
public class FieldAddProcessor extends Processor {

  /**
   * 当目标列存在时是否覆盖
   */
  private boolean overwriteIfFieldExist;

  /**
   * 目标列名
   */
  private String targetField;
  /**
   * 处理类
   */
  private FieldAdder fieldAdder;

  /**
   * 构造函数
   *
   * @param overwriteIfFieldExist 字段存在是否覆盖
   * @param targetField 目标字段名称
   * @param fieldAdder 增加处理类
   */
  public FieldAddProcessor(boolean overwriteIfFieldExist, String targetField,
      FieldAdder fieldAdder) {
    this.overwriteIfFieldExist = overwriteIfFieldExist;
    this.targetField = targetField;
    this.fieldAdder = fieldAdder;
    addConfig();
  }

  @Override
  public Map<String, Object> process(Map<String, Object> data) throws Exception {
    //    目标列存在
    if (data.containsKey(targetField) && !overwriteIfFieldExist) {
      throw new FieldExistException(targetField);
    }
    Object result = fieldAdder.fieldAdd(data);
    if (result != null) {
      data.put(targetField, result);
    }
    return data;
  }


  @Override
  public void addConfig() {
    configs.put("overwriteIfFieldExist", overwriteIfFieldExist);
    configs.put("targetField", targetField);
  }
}

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

  private String fixedValue;

  public FixedValueFieldAdder(Map<String, Object> parmas) {
    super(parmas);
    fixedValue = (String) parmas.get("fixedValue");
  }

  @Override
  Object fieldAdd(Map<String, Object> data) {
    return fixedValue;
  }
}

