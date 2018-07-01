package com.weibo.dip.pipeline.processor.merge;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * 列合并处理生成器.
 * Create by hongxun on 2018/6/27
 */
public enum FieldMergeTypeEnum implements TypeEnum {

  StrMerge {
    @Override
    public Merger getMerger(Map<String, Object> parmas) {
      return new StrMerger(parmas);
    }
  },
  ListMerge {
    @Override
    public Merger getMerger(Map<String, Object> parmas) {
     return new ListMerger(parmas);
    }
  },
  MapMerge {
    @Override
    public Merger getMerger(Map<String, Object> parmas) {
      return new MapMerger(parmas);
    }
  },
  SetMerge {
    @Override
    public Merger getMerger(Map<String, Object> parmas) {
      return new SetMerger(parmas);
    }
  };


  private static final Map<String, FieldMergeTypeEnum> types =
      new ImmutableMap.Builder<String, FieldMergeTypeEnum>()
          .put("merge_str", StrMerge)
          .put("merge_list", ListMerge)
          .put("merge_map", MapMerge)
          .put("merge_set", SetMerge)
          .build();

  public Merger getMerger(Map<String, Object> parmas) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static FieldMergeTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
