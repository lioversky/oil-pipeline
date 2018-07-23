package com.weibo.dip.pipeline.processor.add;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/23
 */
public enum DatasetAddTypeEnum implements TypeEnum {

  Copy {
    @Override
    public DatasetAdder getDatasetAdder(Map<String, Object> params) {
      return new CopyDatasetAdder(params);
    }
  },
  CurrentDateStr {
    @Override
    public DatasetAdder getDatasetAdder(Map<String, Object> params) {
      return new CurrentDateStrDatasetAdder(params);
    }
  },
  CurrentTimestamp {
    @Override
    public DatasetAdder getDatasetAdder(Map<String, Object> params) {
      return new CurrentTimestampDatasetAdder(params);
    }
  },
  CurrentUnixTimestamp {
    @Override
    public DatasetAdder getDatasetAdder(Map<String, Object> params) {
      return new CurrentUnixTimestampDatasetAdder(params);
    }
  },
  FixedValue {
    @Override
    public DatasetAdder getDatasetAdder(Map<String, Object> params) {
      return new FixedValueDatasetAdder(params);
    }
  };
  private static final Map<String, DatasetAddTypeEnum> types =
      new ImmutableMap.Builder<String, DatasetAddTypeEnum>()
          .put("fieldadd_copy", Copy)
          .put("fieldadd_datestr", CurrentDateStr)
          .put("fieldadd_timestamp", CurrentTimestamp)
          .put("fieldadd_unixtimestamp", CurrentUnixTimestamp)
          .put("fieldadd_fixedvalue", FixedValue)
          .build();

  public DatasetAdder getDatasetAdder(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static DatasetAddTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
