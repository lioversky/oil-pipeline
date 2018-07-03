package com.weibo.dip.pipeline.job;

import com.codahale.metrics.Counter;
import com.weibo.dip.pipeline.metrics.MetricSystem;
import com.weibo.dip.pipeline.stage.Stage;
import java.util.List;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/2
 */
public class PipelineJob extends Job {

  /**
   * 处理stage列表，可以嵌套结构
   */
  private List<Stage> stageList;
  /**
   * 记录出错数据量
   */
  private Counter errorCounter = new Counter();
  /**
   * 记录处理成空的数据量
   */
  private Counter nullCounter = new Counter();
  /**
   * 记录数据总量
   */
  private Counter dataCounter = new Counter();

  public PipelineJob(Map<String, Object> configMap) {
    try {
      List<Map<String, Object>> stagesConfigList = (List<Map<String, Object>>) configMap
          .get("stages");
      MetricSystem metricSystem = MetricSystem.getMetricSystem();
      stageList = Stage.createStage(stagesConfigList, metricSystem.getMetricRegistry());
      metricSystem.registry("data-counter", dataCounter);
      metricSystem.registry("error-counter", errorCounter);
      metricSystem.registry("null-counter", nullCounter);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * 数据处理
   * @param data 处理数据
   * @return 处理结果
   * @throws Exception 异常
   */
  @Override
  public Map<String, Object> processJob(Map<String, Object> data) throws Exception {
    dataCounter.inc();
    for (Stage stage : stageList) {
      try {
        data = stage.processStage(data);
        if (data == null) {
          nullCounter.inc();
          break;
        }
      } catch (Exception e) {
        errorCounter.inc();
        e.printStackTrace();
      }
    }
    return data;
  }

  private void sendErrorData(){

  }
}
