package com.weibo.dip.pipeline.processor.convert;

import com.weibo.dip.util.MD5Util;
import java.util.Map;

/**
 * Create by hongxun on 2018/8/8
 */
public class MD5Convertor extends ConvertProcessor {

  public MD5Convertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public String converte(Object data) {
    return MD5Util.md5((String) data);
  }
}
