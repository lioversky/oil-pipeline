package com.weibo.dip.pipeline.processor.convert;

import com.weibo.dip.util.NumberUtil;
import java.util.Map;

/**
 * Create by hongxun on 2018/8/8
 */
public class FloatConvertor extends ConvertProcessor {

  public FloatConvertor(Map<String, Object> params) {
    super(params);
  }

  public Float converte(Object data) {
    return NumberUtil.parseFloat(data);
  }
}
