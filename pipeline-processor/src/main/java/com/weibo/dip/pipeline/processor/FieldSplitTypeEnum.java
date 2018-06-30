package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/6/29
 */
public enum FieldSplitTypeEnum implements TypeEnum {

  StrSpliter {
    @Override
    public Spliter getSpliter(Map<String, Object> parmas) {

      return new StrSpliter(parmas);
    }
  }, ListSpliter {
    @Override
    public Spliter getSpliter(Map<String, Object> parmas) {
      return new ListSpliter(parmas);
    }
  }, ArraySpliter {
    @Override
    public Spliter getSpliter(Map<String, Object> parmas) {
      return new ArraySpliter(parmas);
    }
  };

  private static final Map<String, FieldSplitTypeEnum> types = ImmutableMap.of(
      "split_str", StrSpliter,
      "split_list", ListSpliter,
      "split_array", ArraySpliter
  );

  public Spliter getSpliter(Map<String, Object> parmas) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static FieldSplitTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
