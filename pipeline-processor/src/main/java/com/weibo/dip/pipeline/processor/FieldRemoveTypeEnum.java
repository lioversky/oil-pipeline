package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * 列移除处理生成器.
 * Create by hongxun on 2018/6/27
 */

public enum FieldRemoveTypeEnum implements TypeEnum {
  Keep {
    @Override
    public FieldRemover getFieldRemover(Map<String, Object> parmas) {

      return new KeepFieldRemover(parmas);
    }
  },
  Remove {
    @Override
    public FieldRemover getFieldRemover(Map<String, Object> parmas) {
      return new RemoveFieldRemover(parmas);
    }
  },
  RemoveNull {
    @Override
    public FieldRemover getFieldRemover(Map<String, Object> parmas) {
      return new RemoveNullFieldRemover(parmas);
    }
  };

  private static final Map<String, FieldRemoveTypeEnum> types = ImmutableMap.of(
      "remove_keep", Keep,
      "remove_remove", Remove,
      "remove_remove_null", RemoveNull
  );

  public FieldRemover getFieldRemover(Map<String, Object> parmas) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static FieldRemoveTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
