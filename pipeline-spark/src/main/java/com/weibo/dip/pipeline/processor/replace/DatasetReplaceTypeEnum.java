package com.weibo.dip.pipeline.processor.replace;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/19
 */
public enum DatasetReplaceTypeEnum implements TypeEnum {
  Base64Encode {
    @Override
    public DatasetReplacer getDatasetReplacer(Map<String, Object> params) {
      return new Base64EncodeReplacer(params);
    }
  },
  Base64Decode {
    @Override
    public DatasetReplacer getDatasetReplacer(Map<String, Object> params) {
      return new Base64DecodeReplacer(params);
    }
  },
  Regex {
    @Override
    public DatasetReplacer getDatasetReplacer(Map<String, Object> params) {
      return new RegexReplacer(params);
    }
  };


  private static final Map<String, DatasetReplaceTypeEnum> types =
      new ImmutableMap.Builder<String, DatasetReplaceTypeEnum>()
          .put("replace_base64encode", Base64Encode)
          .put("replace_base64decode", Base64Decode)
          .put("replace_regex", Regex)
          .build();

  public DatasetReplacer getDatasetReplacer(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static DatasetReplaceTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
