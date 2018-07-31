package com.weibo.dip.pipeline;

/**
 * sink顶层抽象类
 * 计划输出包括：hdfs,console,kafka,summon(es),influxdb,db,http
 * Create by hongxun on 2018/7/6
 */

public abstract class Sink<T> extends Step {

  public abstract void write(T t);
}
