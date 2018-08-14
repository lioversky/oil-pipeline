package com.weibo.dip.pipeline.exception;

/**
 * 属性不能为空异常.
 * Create by hongxun on 2018/7/3
 */
public class AttrCanNotBeNullException extends RuntimeException {

  public AttrCanNotBeNullException(String message) {
    super(message);
  }

}
