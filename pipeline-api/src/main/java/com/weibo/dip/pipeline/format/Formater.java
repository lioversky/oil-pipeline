package com.weibo.dip.pipeline.format;

import java.io.Serializable;
import java.util.Map;

/**
 * 格式化成对应的结构抽象类.
 * Create by hongxun on 2018/8/1
 */
public abstract class Formater<T> implements Serializable {

  public Formater(Map<String, Object> params) {
  }

  public abstract Object parseRow(T t) throws Exception;
}
