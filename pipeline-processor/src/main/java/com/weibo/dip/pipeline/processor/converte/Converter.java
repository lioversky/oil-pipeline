package com.weibo.dip.pipeline.processor.converte;

import com.google.common.collect.Maps;
import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import com.weibo.dip.util.NumberUtil;
import com.weibo.dip.util.StringUtil;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Create by hongxun on 2018/7/1
 */
abstract class Converter extends Configuration {

  public Converter(Map<String, Object> params) {
    if (params != null) {
      addConfigs(params);
    }
  }

  public abstract Object converte(Object data);

}


class IntegerConverter extends Converter {

  public IntegerConverter(Map<String, Object> params) {
    super(params);
  }

  public Integer converte(Object data) {
    return NumberUtil.parseNumber(data);
  }
}

class LongConverter extends Converter {

  public LongConverter(Map<String, Object> params) {
    super(params);
  }

  public Long converte(Object data) {
    return NumberUtil.parseLong(data);
  }
}

class DoubleConverter extends Converter {

  public DoubleConverter(Map<String, Object> params) {
    super(params);
  }

  public Double converte(Object data) {
    return NumberUtil.parseDouble(data);
  }
}

class FloatConverter extends Converter {

  public FloatConverter(Map<String, Object> params) {
    super(params);
  }

  public Float converte(Object data) {
    return NumberUtil.parseFloat(data);
  }
}

class StringConverter extends Converter {

  public StringConverter(Map<String, Object> params) {
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

class ToLowerCaseConverter extends Converter {

  public ToLowerCaseConverter(Map<String, Object> params) {

    super(params);
  }

  @Override
  public Object converte(Object data) {
    return ((String) data).toLowerCase();
  }
}

class ToUpperCaseConverter extends Converter {

  public ToUpperCaseConverter(Map<String, Object> params) {

    super(params);
  }

  @Override
  public Object converte(Object data) {
    return ((String) data).toUpperCase();
  }
}

class StrToArrayConverter extends Converter {

  private String splitStr;

  public StrToArrayConverter(Map<String, Object> params) {
    super(params);
    splitStr = (String) params.get("splitStr");
  }

  @Override
  public Object converte(Object data) {
    return ((String) data).split(splitStr, -1);
  }
}


class UrlArgsConverter extends Converter {

  /**
   * 保留字段
   */
  private List<String> keepFields;

  public UrlArgsConverter(Map<String, Object> params) {
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

class MappingConverter extends Converter {

  private Map<String, Object> mapping;

  @Override
  public Object converte(Object data) {
    return mapping.get(data);
  }

  public MappingConverter(Map<String, Object> params) {
    super(params);
    mapping = (Map<String, Object>) params.get("mapping");
    if (mapping == null) {
      throw new AttrCanNotBeNullException("Mapping can not be null !!!");
    }
  }
}

class IntervalConverter extends Converter {

  public IntervalConverter(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object converte(Object data) {
    return null;
  }
}
