package com.weibo.dip.pipeline.processor;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.util.NumberUtil;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * 类型转换处理器
 * Created by hongxun on 18/6/26.
 */


public class ConverteProcessor extends FieldProcessor {

  private static final long serialVersionUID = 1L;

  private Converter converter;


  public ConverteProcessor(boolean fieldNotExistError, String columnName, Converter converter) {
    super(fieldNotExistError, columnName);
    this.converter = converter;
  }

  @Override
  public Object columnProcess(Object data) throws Exception {
    return converter.converte(data);
  }
}

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

