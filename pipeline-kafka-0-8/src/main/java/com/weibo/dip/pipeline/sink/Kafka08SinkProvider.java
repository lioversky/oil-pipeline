package com.weibo.dip.pipeline.sink;

import java.util.Map;

/**
 * kafka 0.8source生成器
 * Create by hongxun on 2018/8/1
 */
public class Kafka08SinkProvider extends KafkaSinkProvider {

  protected String version = "0.8";
  @Override
  public KafkaDataSink createDataSink(Map<String, Object> params) {
    return new Kafka08DataSyncSink(params);
  }
}
