package com.weibo.dip.pipeline.exception;

public class FieldExistException extends Exception {

  public FieldExistException(String key) {
    super(String.format("key %s exist!!!", key));
  }
}
