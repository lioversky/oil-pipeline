package com.weibo.dip.pipeline.processor.convert;

import java.util.Collection;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Create by hongxun on 2018/8/8
 */
public class StringConvertor extends ConvertProcessor {

  public StringConvertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object converte(Object data) {
    if (data instanceof byte[]) {
      byte[] value = (byte[]) data;
      return new String(value);
    } else if (data instanceof Collection) {
      Collection c = (Collection) data;
      return StringUtils.join(c);
    } else if (data instanceof Map) {
      //todo:map value to str
      return null;
    } else {
      return data.toString();
    }
  }
}
