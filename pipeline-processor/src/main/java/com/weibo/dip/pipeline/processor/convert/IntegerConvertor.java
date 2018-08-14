package com.weibo.dip.pipeline.processor.convert;

import com.weibo.dip.util.NumberUtil;
import java.util.Map;

/**
 * Create by hongxun on 2018/8/8
 */
public class IntegerConvertor extends ConvertProcessor {

  public IntegerConvertor(Map<String, Object> params) {
    super(params);
  }

  public Integer converte(Object data) {
    return NumberUtil.parseNumber(data);
  }
}
