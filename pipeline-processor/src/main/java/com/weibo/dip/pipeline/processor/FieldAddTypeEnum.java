package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * 增加新列处理生成器.
 * Create by hongxun on 2018/6/27
 */

public enum FieldAddTypeEnum implements TypeEnum {

  Copy {
    @Override
    public FieldAdder getFieldAdder(Map<String, Object> parmas) {

      return new CopyFieldAdder(parmas);
    }
  },
  CurrentDateStr {
    @Override
    public FieldAdder getFieldAdder(Map<String, Object> parmas) {

      return new CurrentDateStrFieldAdder(parmas);
    }
  },
  CurrentTimestamp {
    @Override
    public FieldAdder getFieldAdder(Map<String, Object> parmas) {
      return new CurrentTimestampFieldAdder(parmas);
    }
  },
  CurrentUnixTimestamp {
    @Override
    public FieldAdder getFieldAdder(Map<String, Object> parmas) {
      return new CurrentUnixTimestampFieldAdder(parmas);
    }
  },
  FixedValue {
    @Override
    public FieldAdder getFieldAdder(Map<String, Object> parmas) {
      return new FixedValueFieldAdder(parmas);
    }
  };


  private static final Map<String, FieldAddTypeEnum> types =
      new ImmutableMap.Builder<String, FieldAddTypeEnum>()
          .put("fieldadd_copy", Copy)
          .put("fieldadd_datestr", CurrentDateStr)
          .put("fieldadd_timestamp", CurrentTimestamp)
          .put("fieldadd_unixtimestamp", CurrentUnixTimestamp)
          .put("fieldadd_fixedvalue", FixedValue)
          .build();

  public FieldAdder getFieldAdder(Map<String, Object> parmas) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static FieldAddTypeEnum getType(String typeName) {
    return types.get(typeName);
  }

}
