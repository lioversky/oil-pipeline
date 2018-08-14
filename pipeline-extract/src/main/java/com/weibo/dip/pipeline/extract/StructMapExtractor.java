package com.weibo.dip.pipeline.extract;

import com.weibo.dip.pipeline.metrics.MetricsSystem;
import java.util.List;
import java.util.Map;

/**
 * map提取器
 * Create by hongxun on 2018/8/9
 */
public abstract class StructMapExtractor extends Extractor<List<Map<String, Object>>> {

  private String extractResultMetrics;

  /**
   * 构造函数
   *
   * @param params 参数
   */
  public StructMapExtractor(Map<String, Object> params) {
    super(params);
    extractResultMetrics = metricsName + "_extracted";
  }

  @Override
  public List<Map<String, Object>> extract(Object data) {
    // todo metrics name
    MetricsSystem.getMeter(metricsName).mark();
    List<Map<String, Object>> result = extractLine((String) data);
    MetricsSystem.getCounter(extractResultMetrics).inc(result.size());
    return result;
  }

  public abstract List<Map<String, Object>> extractLine(String line);
}
