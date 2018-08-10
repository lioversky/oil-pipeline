package com.weibo.dip.pipeline.extract;

import java.util.List;
import java.util.Map;

/**
 * Create by hongxun on 2018/8/9
 */
public abstract class StructMapExtractor extends Extractor<List<Map<String, Object>>> {

  public StructMapExtractor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public List<Map<String, Object>> extract(Object data) throws Exception {
    return extract((String) data);
  }

  public abstract List<Map<String, Object>> extract(String line);
}
