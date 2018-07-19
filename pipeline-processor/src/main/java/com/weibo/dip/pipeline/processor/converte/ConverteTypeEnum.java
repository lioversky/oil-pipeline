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
    public Converter getConverter(Map<String, Object> params) {
      return new IntegerConverter(params);
    }
  },
  LongType {
    @Override
    public Converter getConverter(Map<String, Object> params) {
      return new LongConverter(params);
    }
  },
  FloatType {
    @Override
    public Converter getConverter(Map<String, Object> params) {
      return new FloatConverter(params);
    }
  },
  DoubleType {
    @Override
    public Converter getConverter(Map<String, Object> params) {
      return new DoubleConverter(params);
    }
  }, ToLowerCase {
    @Override
    public Converter getConverter(Map<String, Object> params) {
      return new ToLowerCaseConverter(params);
    }
  }, ToUpperCase {
    @Override
    public Converter getConverter(Map<String, Object> params) {
      return new ToUpperCaseConverter(params);
    }
  }, StrToArray {
    @Override
    public Converter getConverter(Map<String, Object> params) {
      return new StrToArrayConverter(params);
    }
  }, UrlArgsConverter {
    @Override
    public Converter getConverter(Map<String, Object> params) {
      return new UrlArgsConverter(params);
    }
  };

  private static final Map<String, ConverteTypeEnum> types =
      new ImmutableMap.Builder<String, ConverteTypeEnum>()
          .put("converte_integer", IntegerType)
          .put("converte_long", LongType)
          .put("converte_float", FloatType)
          .put("converte_double", DoubleType)
          .put("converte_tolowercase", ToLowerCase)
          .put("converte_touppercase", ToUpperCase)
          .put("converte_strtoarray", StrToArray)
          .put("converte_urlargs", UrlArgsConverter)
          .build();

  public Converter getConverter(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static ConverteTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
