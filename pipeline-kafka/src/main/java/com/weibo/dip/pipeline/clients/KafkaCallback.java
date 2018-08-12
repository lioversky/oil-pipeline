package com.weibo.dip.pipeline.clients;

/**
 * Create by hongxun on 2018/8/12
 */
public interface KafkaCallback {
  void onCompletion(boolean success, Exception exception);
}
