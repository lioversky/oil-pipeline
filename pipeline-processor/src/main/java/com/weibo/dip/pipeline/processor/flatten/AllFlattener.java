package com.weibo.dip.pipeline.processor.flatten;

import com.google.common.collect.Maps;
import java.util.Map;

/**
 * 展开所有字段
 */
public class AllFlattener extends FlattenProcessor {

  public AllFlattener(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Map<String, Object> flatten(Map<String, Object> data) throws Exception {
    Map<String, Object> result = Maps.newHashMap();
    for (Map.Entry<String, Object> entry : data.entrySet()) {
      result.putAll(flattenMap(entry.getKey(), entry.getValue(), 1));
    }
    return result;
  }
}
