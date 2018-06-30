package com.weibo.dip.pipeline.processor;

import com.weibo.dip.pipeline.configuration.Configuration;
import org.apache.commons.codec.binary.Base64;

/**
 * Base64处理器.
 * 支持byte数组的转码和解码
 * Create by hongxun on 2018/6/27
 */
public class Base64Processor extends FieldProcessor {

  private Base64er base64er;

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


abstract class Base64er extends Configuration {

  abstract byte[] base64Code(byte[] value);
}


class EncodeBase64er extends Base64er {

  @Override
  byte[] base64Code(byte[] value) {
    return Base64.encodeBase64(value);
  }
}

class DecodeBase64er extends Base64er {

  @Override
  byte[] base64Code(byte[] value) {
    return Base64.decodeBase64(value);
  }
}
