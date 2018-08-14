package com.weibo.dip.pipeline.processor.select;

import java.util.Map;

/**
 * 删除指定字段
 */
public class RemoveFieldSelector extends FieldSelectProcessor {

  @Override
  Map<String, Object> fieldRemove(Map<String, Object> data) throws Exception {
    for (String field : fields) {
      data.remove(field);
    }
    return data;
  }

  public RemoveFieldSelector(Map<String, Object> params) {
    super(params);

  }
}
