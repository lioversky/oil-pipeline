package com.weibo.dip.pipeline;

/**
 * sink顶层抽象类
 * 计划输出包括：hdfs,console,kafka,summon(es),influxdb,db,http
 * Create by hongxun on 2018/7/6
 */

public abstract class Sink<T> extends Step {

  /**
   * 写出数据
   * @param t 类型由各实现类指定
   */
  public abstract void write(T t);

  /**
   * 关闭资源方法，如无资源关闭，空实现
   */
  public abstract void stop();
}
