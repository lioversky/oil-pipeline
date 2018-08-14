package com.weibo.dip.pipeline.runner;

import java.io.Serializable;
import java.util.Map;

/**
 * 所有执行器的顶层抽象类.
 * Create by hongxun on 2018/7/5
 */
public abstract class Runner implements Serializable {

  protected Map<String, Object> sourceConfig;
  protected Map<String, Object> processConfig;
  protected Map<String, Object> sinkConfig;

  /**
   * 构造函数
   *
   * @param params 参数
   */
  public Runner(Map<String, Object> params) {
    //source配置
    sourceConfig = (Map<String, Object>) params.get("sourceConfig");
    processConfig = (Map<String, Object>) params.get("processConfig");
    sinkConfig = (Map<String, Object>) params.get("sinkConfig");
  }

  /**
   * 开始方法
   */
  public abstract void start() throws Exception;

  /**
   * 停止方法
   */
  public abstract void stop() throws Exception;
}
