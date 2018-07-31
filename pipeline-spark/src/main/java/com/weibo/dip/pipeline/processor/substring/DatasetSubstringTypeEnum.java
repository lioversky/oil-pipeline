package com.weibo.dip.pipeline.processor.substring;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/23
 */
public enum DatasetSubstringTypeEnum implements TypeEnum {

  Trim {
    @Override
    public DatasetSubstringer getDatasetSubstringer(Map<String, Object> params) {
      return new TrimDatasetSubstringer(params);
    }
  },
  SubstringLR {
    @Override
    public DatasetSubstringer getDatasetSubstringer(Map<String, Object> params) {
      return new LRDatasetSubstringer(params);
    }
  },
  Substring {
    @Override
    public DatasetSubstringer getDatasetSubstringer(Map<String, Object> params) {
      return new FixedDatasetSubstringer(params);
    }
  },
  SubstringIndex {
    @Override
    public DatasetSubstringer getDatasetSubstringer(Map<String, Object> params) {
      return new IndexDatasetSubstringer(params);
    }
  };
  private static final Map<String, DatasetSubstringTypeEnum> types =
      new ImmutableMap.Builder<String, DatasetSubstringTypeEnum>()
          .put("substring_trim", Trim)
          .put("substring_lr", SubstringLR)
          .put("substring_substring", Substring)
          .put("substring_index", SubstringIndex)
          .build();

  public DatasetSubstringer getDatasetSubstringer(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static DatasetSubstringTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
