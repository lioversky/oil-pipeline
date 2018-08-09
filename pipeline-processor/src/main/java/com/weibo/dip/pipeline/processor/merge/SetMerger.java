package com.weibo.dip.pipeline.processor.merge;

import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

/**
 * 合成Set
 */
public class SetMerger extends FieldMergeProcessor {

  public SetMerger(Map<String, Object> params) {
    super(params);
  }

  @Override
  Object merge(Map<String, Object> data) throws Exception {
    Set<Object> set = Sets.newHashSet();
    for (String field : fields) {
      if (data.containsKey(field)) {
        set.add(data.get(field));
      }
    }
    return set;

  }
}
