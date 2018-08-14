package com.weibo.dip.pipeline.clients;

/**
 * PipelineKafkaProducer回调类
 * Create by hongxun on 2018/8/12
 */
public interface KafkaCallback {
  void onCompletion(boolean success, Exception exception);
}
