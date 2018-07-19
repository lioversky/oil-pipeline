package com.weibo.dip.pipeline.processor.md5;

import com.weibo.dip.pipeline.processor.FieldProcessor;
import com.weibo.dip.util.MD5Util;
import java.util.Map;

/**
 * md5转换处理器.
 * Create by hongxun on 2018/6/27
 */
public class MD5EncodeProcessor extends FieldProcessor {

  public MD5EncodeProcessor(Map<String, Object> params) {
    super(params);
  }

  @Override
  protected Object fieldProcess(Object value) throws Exception {
    return MD5Util.md5((String) value);
  }
}
