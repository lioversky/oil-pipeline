package com.weibo.dip.pipeline.processor.split;

import java.util.List;
import java.util.Map;

/**
 * List拆分
 */
public class ListSplitter extends FieldSplitProcessor {

  public ListSplitter(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object split(Object value) throws Exception {
    List list = (List) value;
    return list.toArray(new Object[list.size()]);
  }
}
