package com.weibo.dip.pipeline.sink;

import java.io.Serializable;

/**
 * Create by hongxun on 2018/8/2
 */
public abstract class PipelineKafkaProducer implements Serializable {

  public abstract void send(Object msg);

  public abstract void send(String topic, Object msg);

  public abstract void stop();
}
