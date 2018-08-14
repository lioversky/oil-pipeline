package com.weibo.dip.pipeline.processor.split;

import com.weibo.dip.util.GsonUtil;
import java.util.Map;

/**
 * json拆分
 */
public class JsonSplitter extends FieldSplitProcessor {

  /**
   * json字符串转成Object对象，不指定是数组或是map
   *
   * @param value 拆分字段值
   * @return Object
   * @throws Exception IllegalArgumentException和json解析异常
   */
  @Override
  public Object split(Object value) throws Exception {
    if (value instanceof String) {
      String line = (String) value;
      return GsonUtil.fromJson(line, Object.class);
    } else {
      throw new IllegalArgumentException(String
          .format("Value type for regex split must be String ,but %s", value.getClass().getName()));
    }
  }

  public JsonSplitter(Map<String, Object> params) {
    super(params);
  }
}
