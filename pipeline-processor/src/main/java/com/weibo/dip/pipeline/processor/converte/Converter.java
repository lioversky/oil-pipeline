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

  public abstract Object converte(Object data);

}


class IntegerConverter extends Converter {

  public Integer converte(Object data) {
    return NumberUtil.parseNumber(data);
  }
}

class LongConverter extends Converter {

  public Long converte(Object data) {
    return NumberUtil.parseLong(data);
  }
}

class DoubleConverter extends Converter {

  public Double converte(Object data) {
    return NumberUtil.parseDouble(data);
  }
}

class FloatConverter extends Converter {

  public Float converte(Object data) {
    return NumberUtil.parseFloat(data);
  }
}

class StringConverter extends Converter {

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

