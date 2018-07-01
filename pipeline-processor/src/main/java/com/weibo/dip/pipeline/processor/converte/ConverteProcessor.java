package com.weibo.dip.pipeline.processor.converte;

import com.weibo.dip.pipeline.processor.FieldProcessor;

/**
 * 类型转换处理器
 * Created by hongxun on 18/6/26.
 */


public class ConverteProcessor extends FieldProcessor {

  private static final long serialVersionUID = 1L;

  private Converter converter;


  public ConverteProcessor(boolean fieldNotExistError, String columnName, Converter converter) {
    super(fieldNotExistError, columnName);
    this.converter = converter;
  }

  @Override
  public Object columnProcess(Object data) throws Exception {
    return converter.converte(data);
  }
}
