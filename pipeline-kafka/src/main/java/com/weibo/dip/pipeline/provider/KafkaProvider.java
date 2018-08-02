package com.weibo.dip.pipeline.provider;

import java.io.Serializable;

/**
 * kafka provider 抽象类
 * Create by hongxun on 2018/8/2
 */
public abstract class KafkaProvider implements Serializable {

  /**
   * 记录版本号
   */
  public abstract double getVersion();
}
