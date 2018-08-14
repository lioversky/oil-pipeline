package com.weibo.dip.pipeline.parse;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * 行解析枚举类
 * Create by hongxun on 2018/8/1
 */
public enum RowParserTypeEnum implements TypeEnum {

  Delimiter {
    @Override
    public RowParser getRowParser(Map<String, Object> params) {
      return new DelimiterParser(params);
    }
  },
  JsonStr {
    @Override
    public RowParser getRowParser(Map<String, Object> params) {
      return new JsonStrParser(params);
    }
  },
  Summon {
    @Override
    public RowParser getRowParser(Map<String, Object> params) {
      return new SummonParser(params);
    }
  };
  private static final Map<String, RowParserTypeEnum> types =
      new ImmutableMap.Builder<String, RowParserTypeEnum>()
          .put("parse_delimiter", Delimiter)
          .put("parse_jsonstr", JsonStr)
          .put("parse_summon", Summon)
          .build();

  public RowParser getRowParser(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static RowParserTypeEnum getType(String typeName) {
    return types.get(typeName);
  }

  public static RowParser getRowParserByMap(Map<String, Object> params) {
    String typeName = (String) params.get("type");
    return types.get(typeName).getRowParser(params);
  }
}
