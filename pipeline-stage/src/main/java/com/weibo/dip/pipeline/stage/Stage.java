package com.weibo.dip.pipeline.stage;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;
import java.util.Map;


/**
 * 处理每个阶段内的processor.
 * 监控阶段内的处理时长等统计指标 ，控制各processor的异常
 * 异常数据处理
 * Create by hongxun on 2018/06/28
 */
public abstract class Stage implements Serializable {


  protected MetricRegistry metricRegistry;


  protected String stageId;

  public Stage(MetricRegistry metricRegistry, String stageId) {
    this.metricRegistry = metricRegistry;
    this.stageId = stageId;
  }

  public abstract Map<String, Object> processStage(Map<String, Object> data) throws Exception;

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  public void setMetricRegistry(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  public void sendErrorMessage() {

  }

  /**
   * 递归创建stage.
   *
   * @param stagesConfigList stage配置列表
   * @return stage列表
   * @throws Exception stage类型不存在异常
   */
  @SuppressWarnings({"unchecked"})
  public static List<Stage> createStage(List<Map<String, Object>> stagesConfigList)
      throws Exception {
    List<Stage> result = Lists.newArrayList();
    for (Map<String, Object> stageConfigMap : stagesConfigList) {
      String stageType = (String) stageConfigMap.get("type");
      //创建casewhenStage
      if ("casewhen".equals(stageType)) {
        List<Map<String, Object>> subStagesList = (List<Map<String, Object>>) stageConfigMap
            .get("subStagesList");
        result.add(new CaseWhenStage(subStagesList, createStageId(stageType)));
      } else if ("pipeline".equals(stageType)) {
        List<Map<String, Object>> processorConfigList = (List<Map<String, Object>>) stageConfigMap
            .get("processors");
        result.add(new PipelineStage(processorConfigList, createStageId(stageType)));
      }
    }
    return result;
  }

  private static String createStageId(String stageType) {
    return stageType;
  }

}
