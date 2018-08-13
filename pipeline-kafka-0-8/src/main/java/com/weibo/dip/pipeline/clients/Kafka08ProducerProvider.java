package com.weibo.dip.pipeline.clients;

import com.weibo.dip.pipeline.provider.KafkaProducerProvider;
import java.util.Map;

/**
 * kafka 0.8版本producer生成器
 * Create by hongxun on 2018/8/12
 */
public class Kafka08ProducerProvider extends KafkaProducerProvider {

  @Override
  public PipelineKafkaProducer createProducer(Map<String, Object> params) {
    return new Kafka08Producer(params);
  }

  @Override
  public double getVersion() {
    return 0.8;
  }
}
