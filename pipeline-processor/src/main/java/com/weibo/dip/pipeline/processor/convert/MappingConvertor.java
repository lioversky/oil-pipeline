package com.weibo.dip.pipeline.processor.convert;

import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;

/**
 * Create by hongxun on 2018/8/8
 */
public class MappingConvertor extends ConvertProcessor {

  private Map<String, Object> mapping;

  @Override
  public Object converte(Object data) {
    return mapping.get(data);
  }

  public MappingConvertor(Map<String, Object> params) {
    super(params);
    mapping = (Map<String, Object>) params.get("mapping");
    if (mapping == null) {
      throw new AttrCanNotBeNullException("Mapping can not be null !!!");
    }
  }
}
