package com.weibo.dip.pipeline.processor.select;

import com.google.common.collect.Maps;
import java.util.Map;

/**
 * 保留指定字段
 */
public class KeepFieldSelector extends FieldSelectProcessor {

  @Override
  Map<String, Object> fieldRemove(Map<String, Object> data) throws Exception {
    Map<String, Object> newData = Maps.newHashMap();
    for (String field : fields) {
      newData.put(field, data.get(field));
    }
    return newData;
  }

  public KeepFieldSelector(Map<String, Object> params) {
    super(params);

  }
}
