package com.weibo.dip.pipeline.processor.convert;

import java.util.Map;

/**
 * Create by hongxun on 2018/8/8
 */
public class StrToArrayConvertor extends ConvertProcessor {

  private String splitStr;

  public StrToArrayConvertor(Map<String, Object> params) {
    super(params);
    splitStr = (String) params.get("splitStr");
  }

  @Override
  public Object converte(Object data) {
    return ((String) data).split(splitStr, -1);
  }
}
