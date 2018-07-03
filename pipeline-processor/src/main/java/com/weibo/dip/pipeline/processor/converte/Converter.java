package com.weibo.dip.pipeline.processor.converte;

import com.google.common.collect.Maps;
import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.util.NumberUtil;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Create by hongxun on 2018/7/1
 */
abstract class Converter extends Configuration {

  public Converter(Map<String, Object> parmas) {
    if (parmas != null) {
      addConfigs(parmas);
    }
  }

  public abstract Object converte(Object data);

}


class IntegerConverter extends Converter {

  public IntegerConverter(Map<String, Object> parmas) {
    super(parmas);
  }

  public Integer converte(Object data) {
    return NumberUtil.parseNumber(data);
  }
}

class LongConverter extends Converter {

  public LongConverter(Map<String, Object> parmas) {
    super(parmas);
  }

  public Long converte(Object data) {
    return NumberUtil.parseLong(data);
  }
}

class DoubleConverter extends Converter {

  public DoubleConverter(Map<String, Object> parmas) {
    super(parmas);
  }

  public Double converte(Object data) {
    return NumberUtil.parseDouble(data);
  }
}

class FloatConverter extends Converter {

  public FloatConverter(Map<String, Object> parmas) {
    super(parmas);
  }

  public Float converte(Object data) {
    return NumberUtil.parseFloat(data);
  }
}

class StringConverter extends Converter {

  public StringConverter(Map<String, Object> parmas) {
    super(parmas);
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

  public ToLowerCaseConverter(Map<String, Object> parmas) {

    super(parmas);
  }

  @Override
  public Object converte(Object data) {
    return ((String) data).toLowerCase();
  }
}

class ToUpperCaseConverter extends Converter {

  public ToUpperCaseConverter(Map<String, Object> parmas) {

    super(parmas);
  }

  @Override
  public Object converte(Object data) {
    return ((String) data).toUpperCase();
  }
}

class StrToArrayConverter extends Converter {

  private String splitStr;

  public StrToArrayConverter(Map<String, Object> parmas) {
    super(parmas);
    splitStr = (String) parmas.get("splitStr");
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

  public UrlArgsConverter(Map<String, Object> parmas) {
    super(parmas);
    String fields = (String) parmas.get("keepFields");
    if (StringUtils.isNotEmpty(fields)) {
      keepFields = Arrays.asList(StringUtils.split(fields, ","));
    }
  }

  @Override
  public Object converte(Object object) {
    String urlargs = (String) object;
    Map<String, String> argsMap = Maps.newHashMap();
    if (urlargs != null) {
      String[] params = urlargs.split("&");
      for (int i = 0; i < params.length; i++) {
        String[] p = params[i].split("=");
        if (p.length == 2) {
          if (keepFields == null || keepFields.contains(p[0])) {
            argsMap.put(p[0], p[1]);
          }
        }
      }
    }
    return argsMap;
  }
}