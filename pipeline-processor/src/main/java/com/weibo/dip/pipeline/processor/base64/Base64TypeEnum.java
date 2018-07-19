package com.weibo.dip.pipeline.processor.base64;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Base64处理生成器.
 * Create by hongxun on 2018/6/27
 */
public enum Base64TypeEnum implements TypeEnum {

  Encode {
    @Override
    public Base64er getBase64er(Map<String, Object> params) {
      return new EncodeBase64er();
    }
  },
  Decode {
    @Override
    public Base64er getBase64er(Map<String, Object> params) {
      return new DecodeBase64er();
    }
  };

  private static final  Map<String, Base64TypeEnum> types =
      new ImmutableMap.Builder<String, Base64TypeEnum>()
          .put("base64_encode", Encode)
          .put("base64_decode", Decode)
          .build();

  public Base64er getBase64er(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static Base64TypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
