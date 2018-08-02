package com.weibo.dip.pipeline.processor.converte;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * 类型转换处理生成器.
 * Create by hongxun on 2018/6/27
 */

public enum ConvertTypeEnum implements TypeEnum {

  IntegerType {
    @Override
    public Convertor getConverter(Map<String, Object> params) {
      return new IntegerConvertor(params);
    }
  },
  LongType {
    @Override
    public Convertor getConverter(Map<String, Object> params) {
      return new LongConvertor(params);
    }
  },
  FloatType {
    @Override
    public Convertor getConverter(Map<String, Object> params) {
      return new FloatConvertor(params);
    }
  },
  DoubleType {
    @Override
    public Convertor getConverter(Map<String, Object> params) {
      return new DoubleConvertor(params);
    }
  }, ToLowerCase {
    @Override
    public Convertor getConverter(Map<String, Object> params) {
      return new ToLowerCaseConvertor(params);
    }
  }, ToUpperCase {
    @Override
    public Convertor getConverter(Map<String, Object> params) {
      return new ToUpperCaseConvertor(params);
    }
  }, StrToArray {
    @Override
    public Convertor getConverter(Map<String, Object> params) {
      return new StrToArrayConvertor(params);
    }
  }, UrlArgsConverter {
    @Override
    public Convertor getConverter(Map<String, Object> params) {
      return new UrlArgsConvertor(params);
    }
  }, Base64Encode {
    @Override
    public Convertor getConverter(Map<String, Object> params) {
      return new Base64EncodeConvertor(params);
    }
  },
  Base64Decode {
    @Override
    public Convertor getConverter(Map<String, Object> params) {
      return new Base64DecodeConvertor(params);
    }
  },
  MD5 {
    @Override
    public Convertor getConverter(Map<String, Object> params) {
      return new MD5Convertor(params);
    }
  };

  private static final Map<String, ConvertTypeEnum> types =
      new ImmutableMap.Builder<String, ConvertTypeEnum>()
          .put("converte_integer", IntegerType)
          .put("converte_long", LongType)
          .put("converte_float", FloatType)
          .put("converte_double", DoubleType)
          .put("converte_tolowercase", ToLowerCase)
          .put("converte_touppercase", ToUpperCase)
          .put("converte_strtoarray", StrToArray)
          .put("converte_urlargs", UrlArgsConverter)
          .put("converte_base64encode", Base64Encode)
          .put("converte_base64decode", Base64Decode)
          .put("converte_md5", MD5)
          .build();

  public Convertor getConverter(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static ConvertTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
