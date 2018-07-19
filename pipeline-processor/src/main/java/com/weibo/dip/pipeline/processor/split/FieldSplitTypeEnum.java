package com.weibo.dip.pipeline.processor.split;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/6/29
 */
public enum FieldSplitTypeEnum implements TypeEnum {

  DelimiterSpliter {
    @Override
    public Splitter getSplitter(Map<String, Object> params) {

      return new DelimiterSplitter(params);
    }
  }, ListSpliter {
    @Override
    public Splitter getSplitter(Map<String, Object> params) {
      return new ListSplitter(params);
    }
  }, ArraySpliter {
    @Override
    public Splitter getSplitter(Map<String, Object> params) {
      return new ArraySplitter(params);
    }
  }, RegexSpliter {
    @Override
    public Splitter getSplitter(Map<String, Object> params) {
      return new RegexSplitter(params);
    }
  }, JsonSpliter {
    @Override
    public Splitter getSplitter(Map<String, Object> params) {
      return new JsonSplitter(params);
    }
  };

  private static final Map<String, FieldSplitTypeEnum> types =
      new ImmutableMap.Builder<String, FieldSplitTypeEnum>()
          .put("split_delimiter", DelimiterSpliter)
          .put("split_regex", RegexSpliter)
          .put("split_json", JsonSpliter)
          .put("split_list", ListSpliter)
          .put("split_array", ArraySpliter)
          .build();

  public Splitter getSplitter(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static FieldSplitTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
