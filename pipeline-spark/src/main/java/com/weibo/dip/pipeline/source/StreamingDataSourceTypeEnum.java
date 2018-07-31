package com.weibo.dip.pipeline.source;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/30
 */
public enum StreamingDataSourceTypeEnum implements TypeEnum {

  HdfsTime {
    @Override
    public StreamingDataSource getStreamingDataSource(Map<String, Object> params) {
      return new StreamingHdfsTimeDataSource(params);
    }
  },
  Kafka {
    @Override
    public StreamingDataSource getStreamingDataSource(Map<String, Object> params) {
      return new StreamingKafkaDelegate(params);
    }
  };


  private static final Map<String, StreamingDataSourceTypeEnum> types =
      new ImmutableMap.Builder<String, StreamingDataSourceTypeEnum>()
          .put("hdfstime", HdfsTime)
          .put("kafka", Kafka)
          .build();

  public StreamingDataSource getStreamingDataSource(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }



  public static StreamingDataSource getType(String typeName, Map<String, Object> params) {
    return types.get(typeName).getStreamingDataSource(params);
  }
}
