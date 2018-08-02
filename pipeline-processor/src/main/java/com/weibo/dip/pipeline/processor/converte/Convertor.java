package com.weibo.dip.pipeline.processor.converte;

import com.google.common.collect.Maps;
import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import com.weibo.dip.util.MD5Util;
import com.weibo.dip.util.NumberUtil;
import com.weibo.dip.util.StringUtil;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

/**
 * Create by hongxun on 2018/7/1
 */
abstract class Convertor<T> extends Configuration {

  public Convertor(Map<String, Object> params) {
    if (params != null) {
      addConfigs(params);
    }
  }

  public abstract T converte(T data);

}


class IntegerConvertor extends Convertor {

  public IntegerConvertor(Map<String, Object> params) {
    super(params);
  }

  public Integer converte(Object data) {
    return NumberUtil.parseNumber(data);
  }
}

class LongConvertor extends Convertor {

  public LongConvertor(Map<String, Object> params) {
    super(params);
  }

  public Long converte(Object data) {
    return NumberUtil.parseLong(data);
  }
}

class DoubleConvertor extends Convertor {

  public DoubleConvertor(Map<String, Object> params) {
    super(params);
  }

  public Double converte(Object data) {
    return NumberUtil.parseDouble(data);
  }
}

class FloatConvertor extends Convertor {

  public FloatConvertor(Map<String, Object> params) {
    super(params);
  }

  public Float converte(Object data) {
    return NumberUtil.parseFloat(data);
  }
}

class StringConvertor extends Convertor {

  public StringConvertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object converte(Object data) {
    if (data instanceof byte[]) {
      byte[] value = (byte[]) data;
      return new String(value);
    } else if (data instanceof Collection) {
      Collection c = (Collection) data;
      return StringUtils.join(c);
    } else if (data instanceof Map) {
      //todo:map value to str
      return null;
    } else {
      return data.toString();
    }
  }
}

// todo: byte[]

class ToLowerCaseConvertor extends Convertor {

  public ToLowerCaseConvertor(Map<String, Object> params) {

    super(params);
  }

  @Override
  public Object converte(Object data) {
    return ((String) data).toLowerCase();
  }
}

class ToUpperCaseConvertor extends Convertor {

  public ToUpperCaseConvertor(Map<String, Object> params) {

    super(params);
  }

  @Override
  public Object converte(Object data) {
    return ((String) data).toUpperCase();
  }
}

class StrToArrayConvertor extends Convertor {

  private String splitStr;

  public StrToArrayConvertor(Map<String, Object> params) {
    super(params);
    splitStr = (String) params.get("splitStr");
  }

  @Override
  public Object converte(Object data) {
    return ((String) data).split(splitStr, -1);
  }
}


class UrlArgsConvertor extends Convertor {

  /**
   * 保留字段
   */
  private List<String> keepFields;

  public UrlArgsConvertor(Map<String, Object> params) {
    super(params);
    String fields = (String) params.get("keepFields");
    if (StringUtils.isNotEmpty(fields)) {
      keepFields = Arrays.asList(StringUtils.split(fields, ","));
    }
  }

  @Override
  public Object converte(Object object) {
    Map<String, String> resultMap = Maps.newHashMap();
    String urlargs = (String) object;
    Map<String, String> argsMap = StringUtil.urlArgsSplit(urlargs);
    for (String field : keepFields) {
      if (argsMap.containsKey(field)) {
        resultMap.put(field, argsMap.get(field));
      }
    }
    return resultMap;
  }
}

class MappingConvertor extends Convertor {

  private Map<String, Object> mapping;

  @Override
  public Object converte(Object data) {
    return mapping.get(data);
  }

  public MappingConvertor(Map<String, Object> params) {
    super(params);
    mapping = (Map<String, Object>) params.get("mapping");
    if (mapping == null) {
      throw new AttrCanNotBeNullException("Mapping can not be null !!!");
    }
  }
}

class IntervalConvertor extends Convertor {

  public IntervalConvertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object converte(Object data) {
    return null;
  }
}

/**
 * base64转码器
 */
class Base64EncodeConvertor extends Convertor<byte[]> {

  public Base64EncodeConvertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public byte[] converte(byte[] value) {
    return Base64.encodeBase64(value);
  }
}

/**
 * base64解码器
 */
class Base64DecodeConvertor extends Convertor<byte[]> {

  public Base64DecodeConvertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public byte[] converte(byte[] value) {
    return Base64.decodeBase64(value);
  }
}

class MD5Convertor extends Convertor<String> {

  public MD5Convertor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public String converte(String data) {
    return MD5Util.md5(data);
  }
}