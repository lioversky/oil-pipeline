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
    public Splitter getSplitter(Map<String, Object> parmas) {

      return new DelimiterSplitter(parmas);
    }
  }, ListSpliter {
    @Override
    public Splitter getSplitter(Map<String, Object> parmas) {
      return new ListSplitter(parmas);
    }
  }, ArraySpliter {
    @Override
    public Splitter getSplitter(Map<String, Object> parmas) {
      return new ArraySplitter(parmas);
    }
  }, RegexSpliter {
    @Override
    public Splitter getSplitter(Map<String, Object> parmas) {
      return new RegexSplitter(parmas);
    }
  }, JsonSpliter {
    @Override
    public Splitter getSplitter(Map<String, Object> parmas) {
      return new JsonSplitter(parmas);
    }
  };

  private static final Map<String, FieldSplitTypeEnum> types =
      new ImmutableMap.Builder<String, FieldSplitTypeEnum>()
          .put("split_str", DelimiterSpliter)
          .put("split_regex", RegexSpliter)
          .put("split_json", JsonSpliter)
          .put("split_list", ListSpliter)
          .put("split_array", ArraySpliter)
          .build();

  public Splitter getSplitter(Map<String, Object> parmas) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static FieldSplitTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
