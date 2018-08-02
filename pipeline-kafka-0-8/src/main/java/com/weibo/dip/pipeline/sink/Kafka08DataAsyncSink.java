package com.weibo.dip.pipeline.sink;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * kafka 0.8版本异步sink
 * 异步在调用write方法时并不是直接发送msg，而是写到本地队列中由异步线程发送
 * Create by hongxun on 2018/8/1
 */
public class Kafka08DataAsyncSink extends KafkaDataSink {
  private static BlockingQueue<String> queue = new LinkedBlockingQueue<>(2000000);

  public Kafka08DataAsyncSink(Map<String, Object> params) {
    super(params);
  }

  @Override
  public void write(String msg) {

  }

  @Override
  public void write(String topic, String msg) {

  }

  @Override
  public void stop() {

  }
}
