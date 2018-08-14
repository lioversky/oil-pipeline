package com.weibo.dip.pipeline.stage;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 处理每个阶段内的processor.
 * 监控阶段内的处理时长等统计指标 ，控制各processor的异常
 * 异常数据处理
 * Create by hongxun on 2018/06/28
 */
public abstract class Stage<T> implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Stage.class);


  protected String stageId;

  public Stage(String stageId) {

    this.stageId = stageId;
  }

  public abstract T processStage(T data) throws Exception;


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
      MetricRegistry metricRegistry = new MetricRegistry();
      String stageId = createStageId(stageType);
      //按类型创建stage
      if ("casewhen".equals(stageType)) {
        List<Map<String, Object>> subStagesList = (List<Map<String, Object>>) stageConfigMap
            .get("subStages");
        result.add(new CaseWhenStage(metricRegistry, subStagesList, stageId));
      } else if ("pipeline".equals(stageType)) {
        List<Map<String, Object>> processorConfigList = (List<Map<String, Object>>) stageConfigMap
            .get("processors");
        result.add(new PipelineStageNoMetric(processorConfigList, stageId));
      }
    }
    return result;
  }

  private static AtomicInteger index = new AtomicInteger();

  /**
   * 生成stageId
   *
   * @param stageType stage类型
   * @return stageId
   */
  public static String createStageId(String stageType) {
    return String.format("%s-%d", stageType, index.incrementAndGet());
  }

}
