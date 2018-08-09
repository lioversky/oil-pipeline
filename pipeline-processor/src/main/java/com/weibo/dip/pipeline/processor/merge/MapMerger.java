package com.weibo.dip.pipeline.processor.merge;

import com.google.common.collect.Maps;
import java.util.Map;

/**
 * 合成map
 */

public class MapMerger extends FieldMergeProcessor {

  public MapMerger(Map<String, Object> params) {
    super(params);
  }

  @Override
  Object merge(Map<String, Object> data) throws Exception {
    Map<String, Object> map = Maps.newHashMap();
    for (String field : fields) {
      if (data.containsKey(field)) {
        map.put(field, data.get(field));
      }
    }
    return map;
  }
}
