package com.weibo.dip.pipeline.sink;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/26
 */
public enum DatasetSinkTypeEnum implements TypeEnum {

  Kafka {
    @Override
    public DatasetDataSink getDatasetSink(Map<String, Object> params) {
      return new KafkaDatasetSink(params);
    }
  },
  File {},
  Console {
    @Override
    public DatasetDataSink getDatasetSink(Map<String, Object> params) {
      return new ConsoleDatasetSink(params);
    }
  };

  private static final Map<String, DatasetSinkTypeEnum> types =
      new ImmutableMap.Builder<String, DatasetSinkTypeEnum>()
          .put("sink_kafka", Kafka)
          .put("sink_File", File)
          .put("sink_console", Console)
          .build();

  public DatasetDataSink getDatasetSink(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static DatasetSinkTypeEnum getType(String typeName) {
    return types.get(typeName);
  }

  public static DatasetDataSink getDatasetSinkByMap(Map<String, Object> params) {
    String typeName = (String) params.get("type");
    return types.get(typeName).getDatasetSink(params);
  }
}
