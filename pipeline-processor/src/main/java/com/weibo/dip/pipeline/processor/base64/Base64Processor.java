package com.weibo.dip.pipeline.processor.base64;

import com.weibo.dip.pipeline.processor.FieldProcessor;
import java.util.Map;

/**
 * Base64处理器.
 * 支持byte数组的转码和解码
 * Create by hongxun on 2018/6/27
 */
public class Base64Processor extends FieldProcessor {

  private Base64er base64er;

  public Base64Processor(Map<String, Object> params,
      Base64er base64er) {
    super(params);
    this.base64er = base64er;
  }

  public Base64Processor(boolean fieldNotExistError, String columnName,
      Base64er base64er) {
    super(fieldNotExistError, columnName);
    this.base64er = base64er;
  }

  @Override
  protected Object columnProcess(Object value) throws Exception {
    byte[] data = (byte[]) value;
    return base64er.base64Code(data);
  }
}

