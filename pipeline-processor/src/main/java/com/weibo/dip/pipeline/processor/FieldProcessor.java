package com.weibo.dip.pipeline.processor;

import com.weibo.dip.pipeline.exception.FieldNotExistException;
import java.util.Map;

/**
 * 单列处理处理器抽象类
 * Create by hongxun on 2018/6/27
 */

public abstract class FieldProcessor extends Processor {

  protected String fieldName;
  protected boolean fieldNotExistError;

  public FieldProcessor(Map<String, Object> params) {
    fieldName = (String) params.get("fieldName");
    fieldNotExistError = params.containsKey("fieldNotExistError") && (boolean) params
        .get("fieldNotExistError");
  }

  public FieldProcessor(boolean fieldNotExistError, String columnName) {
    this.fieldName = columnName;
    this.fieldNotExistError = fieldNotExistError;
  }

  @Override
  public Map<String, Object> process(Map<String, Object> data) throws Exception {
    if (data.containsKey(fieldName)) {
      data.put(fieldName, columnProcess(data.get(fieldName)));
      return data;
    } else {
      return dealError(data, fieldName);
    }

  }


  protected Map<String, Object> dealError(Map<String, Object> data, String key) throws Exception {
    if (fieldNotExistError) {
      throw new FieldNotExistException(key);
    } else {
      return data;
    }
  }

  protected abstract Object columnProcess(Object value) throws Exception;
}
