package com.weibo.dip.pipeline.source;

import java.util.Map;

/**
 * 0.8.2版本kafka的source生成器
 * Create by hongxun on 2018/7/27
 */
public class StreamingKafka082SourceProvider extends StreamingKafkaSourceProvider {

  @Override
  public StreamingDataSource createDataSource(Map<String, Object> params) {
    if (!params.containsKey("type") || params.get("type").equals("receiver")) {
      return new StreamingKafkaReceiverDataSource(params);
    } else if ("direct".equals(params.get("type"))) {
      return new StreamingKafkaDirectDataSource(params);
    }

    return null;
  }

  @Override
  public double getVersion() {
    return 0.8;
  }
}
