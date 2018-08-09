package com.weibo.dip.pipeline.processor.convert;

import com.weibo.dip.util.NumberUtil;
import java.util.Map;

/**
 * Create by hongxun on 2018/8/8
 */
public class LongConvertor extends ConvertProcessor {

  public LongConvertor(Map<String, Object> params) {
    super(params);
  }

  public Long converte(Object data) {
    return NumberUtil.parseLong(data);
  }
}
