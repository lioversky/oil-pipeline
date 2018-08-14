package com.weibo.dip.pipeline.exception;

/**
 * 字段已存在异常.
 * Create by hongxun on 2018/7/3
 */
public class FieldExistException extends Exception {

  public FieldExistException(String key) {
    super(String.format("key %s exist!!!", key));
  }
}
