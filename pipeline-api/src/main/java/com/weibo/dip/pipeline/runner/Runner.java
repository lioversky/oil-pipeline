package com.weibo.dip.pipeline.runner;

import java.io.Serializable;

/**
 * Create by hongxun on 2018/7/5
 */
public abstract class Runner implements Serializable {

  public abstract void start() throws Exception;

  public abstract void stop() throws Exception;
}
