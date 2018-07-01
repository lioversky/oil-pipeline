package com.weibo.dip.pipeline.processor.replace;

import com.weibo.dip.pipeline.processor.FieldProcessor;

/**
 * 列替换处理器.
 * Create by hongxun on 2018/6/27
 */
public class ReplaceProcessor extends FieldProcessor {


  private Replacer replacer;


  public ReplaceProcessor(boolean fieldNotExistError, String columnName) {
    super(fieldNotExistError, columnName);
  }

  public ReplaceProcessor(boolean fieldNotExistError, String columnName,
      Replacer replacer) {
    super(fieldNotExistError, columnName);
    this.replacer = replacer;
  }

  @Override
  public Object columnProcess(Object data) throws Exception {
    String oldValue = (String) data;
    return replacer.replace(oldValue);

  }

}




