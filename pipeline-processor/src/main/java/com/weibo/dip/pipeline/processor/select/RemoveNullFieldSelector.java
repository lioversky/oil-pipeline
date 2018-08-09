package com.weibo.dip.pipeline.processor.select;

import java.util.Map;

/**
 * 删除为空的指定字段
 */
public class RemoveNullFieldSelector extends FieldSelectProcessor {

  @Override
  Map<String, Object> fieldRemove(Map<String, Object> data) throws Exception {
    for (String field : fields) {
      if (data.containsKey(field) && data.get(field) == null) {
        data.remove(field);
      }
    }
    return data;
  }

  public RemoveNullFieldSelector(Map<String, Object> params) {
    super(params);

  }
}
