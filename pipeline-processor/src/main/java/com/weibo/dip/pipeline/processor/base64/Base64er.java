package com.weibo.dip.pipeline.processor.base64;

import com.weibo.dip.pipeline.configuration.Configuration;
import org.apache.commons.codec.binary.Base64;

/**
 * Create by hongxun on 2018/7/1
 */
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