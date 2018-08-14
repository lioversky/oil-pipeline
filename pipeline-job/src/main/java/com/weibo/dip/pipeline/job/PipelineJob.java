package com.weibo.dip.pipeline.job;

import com.weibo.dip.pipeline.metrics.MetricsSystem;
import com.weibo.dip.pipeline.stage.Stage;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create by hongxun on 2018/7/2
 */
public class PipelineJob extends Job {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineJob.class);

  /**
   * 处理stage列表，可以嵌套结构
   */
  private List<Stage> stageList;

  /**
   * 记录出错数据量
   */
  private String errorCounterName = "job_error_counter";
  /**
   * 记录处理成空的数据量
   */
  private String nullCounterName = "job_null_counter";

  /**
   * 记录数据总量
   */
  private String dataName = "job_all_data";

  public PipelineJob(Map<String, Object> configMap) {
    try {
      @SuppressWarnings({"unchecked"})
      List<Map<String, Object>> stagesConfigList = (List<Map<String, Object>>) configMap
          .get("stages");
      stageList = Stage.createStage(stagesConfigList);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * 数据处理
   * 只有过滤的才会返回null，处理错误都抛异常
   *
   * @param data 处理数据
   * @return 处理结果
   * @throws Exception 处理异常
   */
  @Override
  public Map<String, Object> processJob(Map<String, Object> data) throws Exception {
    MetricsSystem.getMeter(dataName).mark();
    for (Stage stage : stageList) {
      try {
        data = (Map<String, Object>) stage.processStage(data);
        if (data == null) {
          MetricsSystem.getCounter(nullCounterName).inc();
          break;
        }
      } catch (Exception e) {
        MetricsSystem.getCounter(errorCounterName).inc();
        LOGGER.error("process data error.", e);
        sendErrorData(data.get("_value_"));
        return null;
      }
    }
    return data;
  }

  private void sendErrorData(Object line) {
    LOGGER.error((String) line);
  }
}
