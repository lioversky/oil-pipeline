package com.weibo.dip.pipeline.sink;

import com.weibo.dip.pipeline.util.AsyncKafkaProducerUtil;
import java.util.Map;

/**
 * kafka 异步sink
 * 异步在调用write方法时并不是直接发送msg，而是写到本地队列中由异步线程发送
 * Create by hongxun on 2018/8/1
 */
public class KafkaDataAsyncSink extends KafkaDataSink {

  public KafkaDataAsyncSink(Map<String, Object> params) {
    super(params);
  }

  @Override
  public void write(String s) {
    write(topic, s);
  }

  @Override
  public void write(String topic, String msg) {
    try {
      AsyncKafkaProducerUtil.addMessage(topic, msg, kafkaParams);
    } catch (InterruptedException e) {
      //记录错误信息
      e.printStackTrace();
    }
  }

  @Override
  public void stop() {

  }
}
