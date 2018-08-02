package com.weibo.dip.pipeline.sink;

import java.util.Map;

/**
 * kafka 0.10版本异步sink
 * 异步在调用write方法时并不是直接发送msg，而是写到本地队列中由异步线程发送
 * Create by hongxun on 2018/8/1
 */
public class Kafka010DataAsyncSink extends KafkaDataSink {

  public Kafka010DataAsyncSink(Map<String, Object> params) {
    super(params);
  }

  @Override
  public void write(String s) {

  }

  @Override
  public void write(String topic, String msg) {

  }

  @Override
  public void stop() {

  }
}
