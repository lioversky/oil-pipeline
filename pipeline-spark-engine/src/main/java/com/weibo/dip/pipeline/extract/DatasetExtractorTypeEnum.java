package com.weibo.dip.pipeline.extract;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/5
 */
public enum DatasetExtractorTypeEnum implements TypeEnum {
  Delimiter {
    @Override
    public DatasetExtractor getExtractor(Map<String, Object> params) {
      return new DelimiterDatasetExtractor(params);
    }
  },
  Regex {
    @Override
    public DatasetExtractor getExtractor(Map<String, Object> params) {
      return new RegexDatasetExtractor(params);
    }
  },
  Json {
    @Override
    public DatasetExtractor getExtractor(Map<String, Object> params) {
      return new JsonDatasetExtractor(params);
    }
  };


  private static final Map<String, DatasetExtractorTypeEnum> types =
      new ImmutableMap.Builder<String, DatasetExtractorTypeEnum>()
          .put("extract_delimiter", Delimiter)
          .put("extract_regex", Regex)
          .put("extract_json", Json)
          .build();

  public DatasetExtractor getExtractor(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static DatasetExtractorTypeEnum getType(String typeName) {
    return types.get(typeName);
  }

  public static DatasetExtractor getType(Map<String, Object> params) {
    String typeName = (String) params.get("type");
    return types.get(typeName).getExtractor(params);
  }
}
