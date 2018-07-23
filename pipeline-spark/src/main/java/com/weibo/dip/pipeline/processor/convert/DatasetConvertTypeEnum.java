package com.weibo.dip.pipeline.processor.convert;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/23
 */
public enum DatasetConvertTypeEnum implements TypeEnum {
  UrlArgsConverter {
    @Override
    public DatasetConvertor getDatasetConvertor(Map<String, Object> params) {
      return new UrlArgsDatasetConvertor(params);
    }
  };


  private static final Map<String, DatasetConvertTypeEnum> types =
      new ImmutableMap.Builder<String, DatasetConvertTypeEnum>()
//          .put("converte_integer", IntegerType)
//          .put("converte_long", LongType)
//          .put("converte_float", FloatType)
//          .put("converte_double", DoubleType)
//          .put("converte_tolowercase", ToLowerCase)
//          .put("converte_touppercase", ToUpperCase)
//          .put("converte_strtoarray", StrToArray)
          .put("converte_urlargs", UrlArgsConverter)
          .build();

  public DatasetConvertor getDatasetConvertor(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static DatasetConvertTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
