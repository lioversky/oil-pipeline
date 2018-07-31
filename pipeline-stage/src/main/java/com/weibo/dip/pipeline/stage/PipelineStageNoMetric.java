package com.weibo.dip.pipeline.stage;

import com.codahale.metrics.Timer;
import com.weibo.dip.pipeline.metrics.MetricSystem;
import java.util.List;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/30
 */
public class PipelineStageNoMetric extends PipelineStage {

  public PipelineStageNoMetric(List<Map<String, Object>> processorsCofnigList, String stageId) {
    super(processorsCofnigList, stageId);
  }

  @Override
  public Timer getStageTimer() {
    return MetricSystem.getMetricSystem().getMetricRegistry()
        .timer(String.format("%s_timer", stageId));
  }
}
