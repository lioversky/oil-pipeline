package com.weibo.dip.pipeline.processor.split;

import com.weibo.dip.pipeline.exception.FieldExistException;
import com.weibo.dip.pipeline.exception.FieldNotExistException;
import com.weibo.dip.pipeline.processor.Processor;
import java.util.Map;

/**
 * 列拆分处理器.
 * Create by hongxun on 2018/6/29
 */
public class FieldSplitProcessor extends Processor {


  private String[] targetFields;
  private String fieldName;
  protected boolean fieldNotExistError;
  private boolean overwriteIfFieldExist;
  private Splitter splitter;

  public FieldSplitProcessor(String[] targetFields, String fieldName, boolean fieldNotExistError,
      boolean overwriteIfFieldExist, Splitter splitter) {
    this.targetFields = targetFields;
    this.fieldName = fieldName;
    this.fieldNotExistError = fieldNotExistError;
    this.overwriteIfFieldExist = overwriteIfFieldExist;
    this.splitter = splitter;
  }

  /**
   * 分割成多列.
   *
   * @param data 原始数据
   * @return 分隔后结果
   * @throws Exception 异常
   */
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
        Object[] values = (Object[]) splitter.split(value);
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

