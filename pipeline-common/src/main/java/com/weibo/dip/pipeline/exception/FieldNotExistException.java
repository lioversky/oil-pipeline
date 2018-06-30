package com.weibo.dip.pipeline.exception;

public class FieldNotExistException extends Exception {

  public FieldNotExistException(String key) {
    super(String.format("key %s don't exist!!", key));
  }
}
