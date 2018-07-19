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
    public Merger getMerger(Map<String, Object> params) {
      return new StrMerger(params);
    }
  },
  ListMerge {
    @Override
    public Merger getMerger(Map<String, Object> params) {
      return new ListMerger(params);
    }
  },
  MapMerge {
    @Override
    public Merger getMerger(Map<String, Object> params) {
      return new MapMerger(params);
    }
  },
  SetMerge {
    @Override
    public Merger getMerger(Map<String, Object> params) {
      return new SetMerger(params);
    }
  };


  private static final Map<String, FieldMergeTypeEnum> types =
      new ImmutableMap.Builder<String, FieldMergeTypeEnum>()
          .put("merge_str", StrMerge)
          .put("merge_list", ListMerge)
          .put("merge_map", MapMerge)
          .put("merge_set", SetMerge)
          .build();

  public Merger getMerger(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static FieldMergeTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
