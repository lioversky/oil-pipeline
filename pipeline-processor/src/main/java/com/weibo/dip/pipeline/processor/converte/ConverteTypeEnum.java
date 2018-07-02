package com.weibo.dip.pipeline.processor.converte;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;
/**
 * 类型转换处理生成器.
 * Create by hongxun on 2018/6/27
 */

public enum ConverteTypeEnum implements TypeEnum {

  IntegerType {
    @Override
    public Converter getConverter(Map<String, Object> parmas) {
      return new IntegerConverter(parmas);
    }
  },
  LongType {
    @Override
    public Converter getConverter(Map<String, Object> parmas) {
      return new LongConverter(parmas);
    }
  },
  FloatType {
    @Override
    public Converter getConverter(Map<String, Object> parmas) {
      return new FloatConverter(parmas);
    }
  },
  DoubleType {
    @Override
    public Converter getConverter(Map<String, Object> parmas) {
      return new DoubleConverter(parmas);
    }
  };

  private static final Map<String, ConverteTypeEnum> types =
      new ImmutableMap.Builder<String, ConverteTypeEnum>()
          .put("converte_integer", IntegerType)
          .put("converte_long", LongType)
          .put("converte_float", FloatType)
          .put("converte_double", DoubleType)
          .build();

  public Converter getConverter(Map<String, Object> parmas) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static ConverteTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
