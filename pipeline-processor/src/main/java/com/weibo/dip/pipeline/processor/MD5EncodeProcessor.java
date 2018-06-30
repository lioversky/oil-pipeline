package com.weibo.dip.pipeline.processor;

import com.weibo.dip.util.MD5Util;

/**
 * md5转换处理器.
 * Create by hongxun on 2018/6/27
 */
public class MD5EncodeProcessor extends FieldProcessor {

  public MD5EncodeProcessor(boolean fieldNotExistError, String columnName) {
    super(fieldNotExistError, columnName);
  }

  @Override
  protected Object columnProcess(Object value) throws Exception {
    return MD5Util.md5((String) value);
  }
}
