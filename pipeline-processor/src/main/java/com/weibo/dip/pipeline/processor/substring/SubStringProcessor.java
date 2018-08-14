package com.weibo.dip.pipeline.processor.substring;

import com.weibo.dip.pipeline.processor.FieldProcessor;
import java.util.Map;

/**
 * 列截取处理器.
 * Create by hongxun on 2018/6/27
 */
public abstract class SubStringProcessor extends FieldProcessor {


  public SubStringProcessor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object fieldProcess(Object data) throws Exception {
    return substring((String) data);
  }

  abstract String substring(String value) throws Exception;
}

