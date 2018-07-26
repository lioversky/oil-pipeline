package com.weibo.dip.pipeline;


public abstract class Sink<T> extends Step {

  public abstract void write(T t);
}
