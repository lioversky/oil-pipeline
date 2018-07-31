package com.weibo.dip.pipeline.extract;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/5
 */
public enum ExtractorTypeEnum implements TypeEnum {
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

  private static final Map<String, ExtractorTypeEnum> types =
      new ImmutableMap.Builder<String, ExtractorTypeEnum>()
          .put("extract_delimiter", Delimiter)
          .put("extract_regex", Regex)
          .put("extract_multiple", Multiple)
          .build();

  public Extractor getExtractor(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static ExtractorTypeEnum getType(String typeName) {
    return types.get(typeName);
  }

  public static Extractor getType(Map<String, Object> params) {
    String typeName = (String) params.get("type");
    return types.get(typeName).getExtractor(params);
  }
}
