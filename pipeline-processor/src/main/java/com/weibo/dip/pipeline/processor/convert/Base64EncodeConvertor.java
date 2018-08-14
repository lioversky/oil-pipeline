package com.weibo.dip.pipeline.processor.convert;

import java.nio.charset.Charset;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

/**
 * base64转码器
 */
public class Base64EncodeConvertor extends ConvertProcessor {

  public Base64EncodeConvertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object converte(Object data) {
    String value = (String) data;
    return Base64.encodeBase64String(value.getBytes());

  }
}
