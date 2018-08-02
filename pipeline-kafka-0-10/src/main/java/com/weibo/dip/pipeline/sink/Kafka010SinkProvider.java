package com.weibo.dip.pipeline.sink;

import java.util.Map;

/**
 * kafka 0.10版本sink生成器
 * Create by hongxun on 2018/8/1
 */
public class Kafka010SinkProvider extends KafkaSinkProvider {

  protected String version = "0.10";

  @Override
  public KafkaDataSink createDataSink(Map<String, Object> params) {
    String sync = (String) params.get("sync");
    if (sync == null || "true".equals(sync)) {
      return new Kafka010DataSyncSink(params);
    } else {
      return new Kafka010DataAsyncSink(params);
    }
  }

  @Override
  public double getVersion() {
    return 0.10;
  }
}
