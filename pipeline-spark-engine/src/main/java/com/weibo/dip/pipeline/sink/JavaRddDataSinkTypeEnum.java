package com.weibo.dip.pipeline.sink;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/26
 */
public enum JavaRddDataSinkTypeEnum implements TypeEnum {

  Kafka {
    @Override
    public JavaRddDataSink getRddDataSink(Map<String, Object> params) {
      return new KafkaRddDataSink(params);
    }
  },
  Console {
    @Override
    public JavaRddDataSink getRddDataSink(Map<String, Object> params) {
      return new ConsoleRddDataSink(params);
    }
  },
  File {},
  Distribution {
    @Override
    public JavaRddDataSink getRddDataSink(Map<String, Object> params) {
      return new DistributeKafkaRddDataSink(params);
    }
  };

  private static final Map<String, JavaRddDataSinkTypeEnum> types =
      new ImmutableMap.Builder<String, JavaRddDataSinkTypeEnum>()
          .put("sink_kafka", Kafka)
          .put("sink_File", File)
          .put("sink_console", Console)
          .put("sink_distribution", Distribution)
          .build();

  public JavaRddDataSink getRddDataSink(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static JavaRddDataSinkTypeEnum getType(String typeName) {
    return types.get(typeName);
  }

  public static JavaRddDataSink getRddDataSinkByMap(Map<String, Object> params) {
    String typeName = (String) params.get("type");
    return types.get(typeName).getRddDataSink(params);
  }
}
