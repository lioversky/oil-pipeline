package com.weibo.dip.pipeline.processor.merge;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;

/**
 * 合成list
 */
public class ListMerger extends FieldMergeProcessor {

  //  默认值false
  private boolean keepIfNull;

  public ListMerger(Map<String, Object> params) {
    super(params);
    keepIfNull = params.containsKey("keepIfNull") && (boolean) params.get("keepIfNull");
  }

  @Override
  Object merge(Map<String, Object> data) throws Exception {
    List<Object> list = Lists.newArrayList();
    for (String field : fields) {
      if (!data.containsKey(field) && !keepIfNull) {
        continue;
      }
      list.add(data.get(field));
    }
    return list;
  }
}
