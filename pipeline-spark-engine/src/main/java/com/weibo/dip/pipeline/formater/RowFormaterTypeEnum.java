package com.weibo.dip.pipeline.formater;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * 行解析枚举类
 * Create by hongxun on 2018/8/1
 */
public enum RowFormaterTypeEnum implements TypeEnum {

  Delimiter {
    @Override
    public RowFormater getRowParser(Map<String, Object> params) {
      return new DelimiterFormater(params);
    }
  },
  JsonStr {
    @Override
    public RowFormater getRowParser(Map<String, Object> params) {
      return new JsonStrFormater(params);
    }
  },
  Summon {
    @Override
    public RowFormater getRowParser(Map<String, Object> params) {
      return new SummonFormater(params);
    }
  };
  private static final Map<String, RowFormaterTypeEnum> types =
      new ImmutableMap.Builder<String, RowFormaterTypeEnum>()
          .put("parse_delimiter", Delimiter)
          .put("parse_jsonstr", JsonStr)
          .put("parse_summon", Summon)
          .build();

  public RowFormater getRowParser(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static RowFormaterTypeEnum getType(String typeName) {
    return types.get(typeName);
  }

  public static RowFormater getRowParserByMap(Map<String, Object> params) {
    String typeName = (String) params.get("type");
    return types.get(typeName).getRowParser(params);
  }
}
