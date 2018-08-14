package com.weibo.dip.pipeline.processor.split;

import java.util.Map;

/**
 * Array拆分
 */
public class ArraySplitter extends FieldSplitProcessor {

  public ArraySplitter(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object split(Object value) throws Exception {
    return value;
  }
}
