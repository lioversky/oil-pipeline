package com.weibo.dip.pipeline.processor.split;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/23
 */
public enum DatasetSplitTypeEnum implements TypeEnum {

  DelimiterSpliter {
    @Override
    public DatasetSpliter getDatasetSpliter(Map<String, Object> params) {
      return new DelimiterDatasetSplitter(params);
    }
  };
  private static final Map<String, DatasetSplitTypeEnum> types =
      new ImmutableMap.Builder<String, DatasetSplitTypeEnum>()
          .put("split_delimiter", DelimiterSpliter)
//          .put("split_regex", RegexSpliter)
//          .put("split_json", JsonSpliter)
//          .put("split_list", ListSpliter)
//          .put("split_array", ArraySpliter)
          .build();

  public DatasetSpliter getDatasetSpliter(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static DatasetSplitTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
