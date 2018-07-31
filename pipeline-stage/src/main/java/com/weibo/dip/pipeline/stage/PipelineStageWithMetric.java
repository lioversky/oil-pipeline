package com.weibo.dip.pipeline.stage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.List;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/30
 */
public class PipelineStageWithMetric extends PipelineStage {

  protected MetricRegistry metricRegistry;
  private Timer stageTimer = metricRegistry.timer(String.format("%s_timer", stageId));

  public PipelineStageWithMetric(List<Map<String, Object>> processorsCofnigList, String stageId) {
    super(processorsCofnigList, stageId);
    this.metricRegistry = new MetricRegistry();
  }

  @Override
  public Map<String, Object> processStage(Map<String, Object> data) throws Exception {
    return null;
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  public void setMetricRegistry(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  @Override
  public Timer getStageTimer() {
    return stageTimer;
  }
}
