package com.weibo.dip.pipeline.stage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.collect.Maps;
import com.weibo.dip.pipeline.condition.Condition;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用于执行判断，在case中判断，在代码段中执行processor.
 * <p>CaseWhenStage</p> 的执行段为stage列表，可以包含pipeline，也可包含其它casewhen
 * Create by hongxun on 2018/6/29
 */
public class CaseWhenStage extends Stage<Map<String, Object>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CaseWhenStage.class);

  /**
   * Condition和对应执行StageList
   */
  private LinkedHashMap<Condition, List<Stage>> casewhenMap;

  private Timer stageTimer;

  /**
   * 构造函数，创建此stage内的条件可执行stage列表
   *
   * @param params 构造参数
   * @param stageId stageId唯一标识
   */
  public CaseWhenStage(MetricRegistry registry, List<Map<String, Object>> params, String stageId)
      throws Exception {
    super(stageId);
    stageTimer = registry.timer(String.format("%s_timer", stageId));
    casewhenMap = create(params);
  }

  /**
   * 根据配置创建Condition和subStageList.
   *
   * @param params 配置
   * @return Map
   */
  @SuppressWarnings({"unchecked"})
  private LinkedHashMap<Condition, List<Stage>> create(List<Map<String, Object>> params)
      throws Exception {
    LinkedHashMap<Condition, List<Stage>> map = Maps.newLinkedHashMap();

    for (Map<String, Object> param : params) {
      Map<String, Object> conditionParam = (Map<String, Object>) param.get("condition");
      List<Map<String, Object>> stagesConfigList = (List<Map<String, Object>>) param
          .get("stages");
      LOGGER.info(String
          .format("%s condition:%s, stage size: %d", stageId, conditionParam,
              stagesConfigList.size()));
      List<Stage> subStageList = Stage.createStage(stagesConfigList);
      map.put(Condition.createCondition(conditionParam), subStageList);

    }
    return map;
  }

  /**
   * 执行casewhen的stage，遍历map，如果满足条件则执行，执行完跳出，否则向下判断.
   *
   * @param data 待处理数据
   * @return 处理结果
   * @throws Exception 异常
   */
  @Override
  public Map<String, Object> processStage(Map<String, Object> data) throws Exception {
    if (data == null) {
      return null;
    }
    Context context = stageTimer.time();
    try {
      //顺序执行condition和对应的pipelinestage，但只执行condition为true的stage，执行完跳出
      for (Map.Entry<Condition, List<Stage>> entry : casewhenMap.entrySet()) {
        Condition condition = entry.getKey();
        if (condition.conditional(data)) {
          for (Stage stage : entry.getValue()) {
            data = (Map<String, Object>) stage.processStage(data);
          }
          break;
        }
      }
    } catch (Exception e) {
      throw e;
    } finally {
      context.stop();
    }
    return data;
  }
}
