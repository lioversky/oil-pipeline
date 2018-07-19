package com.weibo.dip.pipeline.processor.remove;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * 列移除处理生成器.
 * Create by hongxun on 2018/6/27
 */

public enum FieldRemoveTypeEnum implements TypeEnum {
  Keep {
    @Override
    public Remover getFieldRemover(Map<String, Object> params) {

      return new KeepFieldRemover(params);
    }
  },
  Remove {
    @Override
    public Remover getFieldRemover(Map<String, Object> params) {
      return new RemoveFieldRemover(params);
    }
  },
  RemoveNull {
    @Override
    public Remover getFieldRemover(Map<String, Object> params) {
      return new RemoveNullFieldRemover(params);
    }
  };

  private static final Map<String, FieldRemoveTypeEnum> types = ImmutableMap.of(
      "remove_keep", Keep,
      "remove_remove", Remove,
      "remove_remove_null", RemoveNull
  );

  public Remover getFieldRemover(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static FieldRemoveTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
