package com.weibo.dip.pipeline.extract;

import com.weibo.dip.pipeline.metrics.MetricsSystem;
import java.util.List;
import java.util.Map;

/**
 * Create by hongxun on 2018/8/9
 */
public abstract class StructMapExtractor extends Extractor<List<Map<String, Object>>> {

  private String extractResultMetrics;

  public StructMapExtractor(Map<String, Object> params) {
    super(params);
    extractResultMetrics = metricsName + "_extracted";
  }

  @Override
  public List<Map<String, Object>> extract(Object data) throws Exception {
    // todo metrics name
    MetricsSystem.getMeter(metricsName).mark();
    List<Map<String, Object>> result = extract((String) data);
    MetricsSystem.getCounter(extractResultMetrics).inc(result.size());
    return result;
  }

  public abstract List<Map<String, Object>> extract(String line);
}
