package com.weibo.dip.pipeline.processor.converte;

import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.util.NumberUtil;
import java.util.Collection;
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

