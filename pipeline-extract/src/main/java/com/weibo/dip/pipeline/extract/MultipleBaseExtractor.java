package com.weibo.dip.pipeline.extract;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 默认提供多个exacter的整合
 */
public abstract class MultipleBaseExtractor extends StructMapExtractor {

  protected List<StructMapExtractor> exacters;

  public MultipleBaseExtractor(Map<String, Object> jsonMap) {
    super(jsonMap);
    exacters = new ArrayList<>();
    List<Map<String, Object>> exactersMap = (ArrayList<Map<String, Object>>) jsonMap
        .get("extractors");
    for (Map<String, Object> map : exactersMap) {
      exacters.add((StructMapExtractor) Extractor.createExtractor(map));
    }
  }
}
