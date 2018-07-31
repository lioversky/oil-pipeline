package com.weibo.dip.pipeline.source;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/30
 */
public enum DatasetSourceTypeEnum implements TypeEnum {
  File {
    @Override
    public DatasetSource getDatasetSource(Map<String, Object> params) {
      return new DatasetFileSource(params);
    }
  },
  Kafka {
    @Override
    public DatasetSource getDatasetSource(Map<String, Object> params) {
      return new DatasetKafkaSourceDelegate(params);
    }
  };
  private static final Map<String, DatasetSourceTypeEnum> types =
      new ImmutableMap.Builder<String, DatasetSourceTypeEnum>()
          .put("file", File)
          .put("kafka", Kafka)
          .build();

  public DatasetSource getDatasetSource(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }


  public static DatasetSource getType(String typeName, Map<String, Object> params) {
    return types.get(typeName).getDatasetSource(params);
  }
}
