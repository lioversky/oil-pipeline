package com.weibo.dip.pipeline.processor.convert;

import java.util.Map;

/**
 * Create by hongxun on 2018/8/8
 */

// todo: byte[]

public class ToLowerCaseConvertor extends ConvertProcessor {

  public ToLowerCaseConvertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object converte(Object data) {
    return ((String) data).toLowerCase();
  }
}