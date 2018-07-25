package com.weibo.dip.pipeline.exception;

/**
 * 字段不存在异常异常.
 * Create by hongxun on 2018/7/3
 */
public class FieldNotExistException extends Exception {

  public FieldNotExistException(String key) {
    super(String.format("key %s don't exist!!", key));
  }
}
