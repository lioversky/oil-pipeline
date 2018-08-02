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
    return new Kafka010DataSyncSink(params);
  }
}
