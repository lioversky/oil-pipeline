package com.weibo.dip.pipeline.runner;

/**
 * Create by hongxun on 2018/7/5
 */
public abstract class Runner {

  public abstract void start() throws Exception;

//  public abstract void run() throws Exception;

  public abstract void stop() throws Exception;
}
