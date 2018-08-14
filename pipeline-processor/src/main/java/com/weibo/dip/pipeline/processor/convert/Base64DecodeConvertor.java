package com.weibo.dip.pipeline.processor.convert;

import java.nio.charset.Charset;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

/**
 * base64解码器
 */
public class Base64DecodeConvertor extends ConvertProcessor {

  public Base64DecodeConvertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  Object converte(Object data) {
    String value = (String) data;
    return StringUtils.toEncodedString(Base64.decodeBase64(value), Charset.forName("utf8"));
  }

}
