package com.weibo.dip.pipeline.processor.converte;

import com.weibo.dip.pipeline.processor.FieldProcessor;
import java.util.Map;

/**
 * 类型转换处理器
 * Created by hongxun on 18/6/26.
 */


public class ConverteProcessor extends FieldProcessor {

  private static final long serialVersionUID = 1L;

  private Converter converter;

  public ConverteProcessor(Map<String, Object> params, Converter converter) {
    super(params);
    this.converter = converter;
  }

  public ConverteProcessor(boolean fieldNotExistError, String columnName, Converter converter) {
    super(fieldNotExistError, columnName);
    this.converter = converter;
  }

  @Override
  public Object columnProcess(Object data) throws Exception {
    return converter.converte(data);
  }
}
