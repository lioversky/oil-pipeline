package com.weibo.dip.pipeline.extract;

import java.util.List;
import java.util.Map;

/**
 * Create by hongxun on 2018/8/9
 */
public class OrderMultipleExtractor extends MultipleBaseExtractor {


  public OrderMultipleExtractor(Map<String, Object> jsonMap) {
    super(jsonMap);
  }

  @Override
  public List<Map<String, Object>> extract(String line) {
    List<Map<String, Object>> records = null;

    for (StructMapExtractor exacter :
        exacters) {
      records = exacter.extract(line);

      if (records != null) {
        break;
      }

    }
    return records;
  }
}
