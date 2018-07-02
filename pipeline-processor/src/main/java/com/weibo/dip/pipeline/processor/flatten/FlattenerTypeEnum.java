package com.weibo.dip.pipeline.processor.flatten;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import com.weibo.dip.pipeline.processor.converte.ConverteTypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/1
 */
public enum FlattenerTypeEnum implements TypeEnum {

  ALL {
    @Override
    public Flattener getFlattener(Map<String, Object> parmas) {
      return new AllFlattener(parmas);
    }
  }, FIELD {
    @Override
    public Flattener getFlattener(Map<String, Object> parmas) {
      return new FieldFlattener(parmas);
    }
  };


  private static final Map<String, FlattenerTypeEnum> types =
      new ImmutableMap.Builder<String, FlattenerTypeEnum>()
          .put("flatten_all", ALL)
          .put("flatten_field", FIELD)
          .build();

  public Flattener getFlattener(Map<String, Object> parmas) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static FlattenerTypeEnum getType(String typeName) {
    return types.get(typeName);
  }

}
