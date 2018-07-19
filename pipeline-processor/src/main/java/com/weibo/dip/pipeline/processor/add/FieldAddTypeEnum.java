package com.weibo.dip.pipeline.processor.add;

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
    public FieldAdder getFieldAdder(Map<String, Object> params) {

      return new CopyFieldAdder(params);
    }
  },
  CurrentDateStr {
    @Override
    public FieldAdder getFieldAdder(Map<String, Object> params) {

      return new CurrentDateStrFieldAdder(params);
    }
  },
  CurrentTimestamp {
    @Override
    public FieldAdder getFieldAdder(Map<String, Object> params) {
      return new CurrentTimestampFieldAdder(params);
    }
  },
  CurrentUnixTimestamp {
    @Override
    public FieldAdder getFieldAdder(Map<String, Object> params) {
      return new CurrentUnixTimestampFieldAdder(params);
    }
  },
  FixedValue {
    @Override
    public FieldAdder getFieldAdder(Map<String, Object> params) {
      return new FixedValueFieldAdder(params);
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

  public FieldAdder getFieldAdder(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static FieldAddTypeEnum getType(String typeName) {
    return types.get(typeName);
  }

}
