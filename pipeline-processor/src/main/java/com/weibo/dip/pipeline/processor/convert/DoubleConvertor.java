package com.weibo.dip.pipeline.processor.convert;

import com.weibo.dip.util.NumberUtil;
import java.util.Map;

/**
 * Create by hongxun on 2018/8/8
 */
public class DoubleConvertor extends ConvertProcessor {

  public DoubleConvertor(Map<String, Object> params) {
    super(params);
  }

  public Double converte(Object data) {
    return NumberUtil.parseDouble(data);
  }
}
