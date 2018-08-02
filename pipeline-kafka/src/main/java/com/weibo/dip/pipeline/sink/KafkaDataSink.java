package com.weibo.dip.pipeline.sink;

import com.weibo.dip.pipeline.Sink;
import java.util.Map;

/**
 * 写出数据到kafka的顶层抽象类.
 * Create by hongxun on 2018/8/1
 */
public abstract class KafkaDataSink extends Sink<String> {

  protected String topic;
  protected Map<String, Object> kafkaParams;

  public KafkaDataSink(Map<String, Object> params) {
    topic = (String) params.get("topic");
    kafkaParams = (Map<String, Object>) params.get("options");
  }

  public abstract void write(String topic, String msg);
}
