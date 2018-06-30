package com.weibo.dip.pipeline.processor;

import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.pipeline.exception.FieldExistException;
import com.weibo.dip.pipeline.exception.FieldNotExistException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * 列拆分处理器.
 * Create by hongxun on 2018/6/29
 */
public class FieldSplitProcessor extends Processor {


  private String[] targetFields;
  private String fieldName;
  protected boolean fieldNotExistError;
  private boolean overwriteIfFieldExist;
  private Spliter spliter;

  public FieldSplitProcessor(String[] targetFields, String fieldName, boolean fieldNotExistError,
      boolean overwriteIfFieldExist, Spliter spliter) {
    this.targetFields = targetFields;
    this.fieldName = fieldName;
    this.fieldNotExistError = fieldNotExistError;
    this.overwriteIfFieldExist = overwriteIfFieldExist;
    this.spliter = spliter;
  }

  @Override
  public Map<String, Object> process(Map<String, Object> data) throws Exception {
    //不允许覆盖且目标列存在，抛异常
    if (!overwriteIfFieldExist) {
      for (String field : targetFields) {
        if (data.containsKey(field)) {
          throw new FieldExistException(field);
        }
      }
    }
    //列不存在抛异常
    if (fieldNotExistError && !data.containsKey(fieldName)) {
      throw new FieldNotExistException(fieldName);
    }
    //列存在时处理
    if (data.containsKey(fieldName)) {
      Object value = data.get(fieldName);
      if (value != null) {
        Object[] values = (Object[]) spliter.split(value);
        if (values != null && values.length > 0) {
          //列数不匹配时异常
          if (targetFields.length != values.length) {
            throw new RuntimeException(String
                .format("Split value length %d is not equal to column length %d",
                    targetFields.length,
                    targetFields.length));
          } else {
            for (int i = 0; i < targetFields.length; i++) {
              data.put(targetFields[i], values[i]);
            }
          }
        }
      }

    }

    return data;
  }
}

abstract class Spliter extends Configuration {

  public Spliter(Map<String, Object> parmas) {
    if (parmas != null) {
      addConfigs(parmas);
    }
  }

  public abstract Object split(Object value) throws Exception;
}

/**
 * 字符串拆分
 */
class StrSpliter extends Spliter {

  private String splitStr;

  public StrSpliter(Map<String, Object> parmas) {
    super(parmas);
    splitStr = (String) parmas.get("splitStr");
  }

  @Override
  public Object split(Object value) throws Exception {
    //    return StringUtils.split((String)value,splitStr);
    return ((String) value).split(splitStr);
  }
}

/**
 * List拆分
 */
class ListSpliter extends Spliter {

  public ListSpliter(Map<String, Object> parmas) {
    super(parmas);
  }

  @Override
  public Object split(Object value) throws Exception {
    List list = (List) value;
    return list.toArray(new Object[list.size()]);
  }
}

/**
 * Array拆分
 */
class ArraySpliter extends Spliter {

  public ArraySpliter(Map<String, Object> parmas) {
    super(parmas);
  }

  @Override
  public Object split(Object value) throws Exception {
    return value;
  }
}