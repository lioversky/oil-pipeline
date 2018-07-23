package com.weibo.dip.pipeline.extract;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/5
 */
public enum ExactorTypeEnum implements TypeEnum {
  Delimiter {
    @Override
    public Extractor getExtractor(Map<String, Object> params) {
      return new DelimiterExacter(params);
    }
  },
  Regex {
    @Override
    public Extractor getExtractor(Map<String, Object> params) {
      return new RegexExtractor(params);
    }
  },
  Multiple {
    @Override
    public Extractor getExtractor(Map<String, Object> params) {
      return new OrderMultipleExtractor(params);
    }
  };

  private static final Map<String, ExactorTypeEnum> types =
      new ImmutableMap.Builder<String, ExactorTypeEnum>()
          .put("exact_delimiter", Delimiter)
          .put("exact_regex", Regex)
          .put("exact_multiple", Multiple)
          .build();

  public Extractor getExtractor(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static ExactorTypeEnum getType(String typeName) {
    return types.get(typeName);
  }

  public static Extractor getType(Map<String, Object> params) {
    String typeName = (String) params.get("type");
    return types.get(typeName).getExtractor(params);
  }
}
