package com.weibo.dip.pipeline.clients;

import java.io.Serializable;
import java.util.Map;

/**
 * Create by hongxun on 2018/8/2
 */
public abstract class PipelineKafkaProducer<K,V> implements Serializable {

  public PipelineKafkaProducer(Map<String, Object> config) {
  }

  public abstract void send(String topic, Object msg,KafkaCallback callback);
  public abstract void send(String topic, Object msg);
  public abstract void stop();
}
