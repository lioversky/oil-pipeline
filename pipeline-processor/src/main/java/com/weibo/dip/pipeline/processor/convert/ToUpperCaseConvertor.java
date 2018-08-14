package com.weibo.dip.pipeline.processor.convert;

import java.util.Map;

/**
 * Create by hongxun on 2018/8/8
 */
public class ToUpperCaseConvertor extends ConvertProcessor {

  public ToUpperCaseConvertor(Map<String, Object> params) {

    super(params);
  }

  @Override
  public Object converte(Object data) {
    return ((String) data).toUpperCase();
  }
}
