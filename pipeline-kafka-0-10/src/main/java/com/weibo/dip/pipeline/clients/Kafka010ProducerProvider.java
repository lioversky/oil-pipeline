package com.weibo.dip.pipeline.clients;

import java.util.Map;

/**
 * Create by hongxun on 2018/8/12
 */
public class Kafka010ProducerProvider extends KafkaProducerProvider {

  @Override
  public PipelineKafkaProducer createProducer(Map<String, Object> params) {
    return new Kafka010Producer(params);
  }

  @Override
  public double getVersion() {
    return 0.10;
  }
}
