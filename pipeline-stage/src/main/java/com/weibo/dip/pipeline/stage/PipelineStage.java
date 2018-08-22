package com.weibo.dip.pipeline.stage;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.weibo.dip.pipeline.metrics.MetricsSystem;
import com.weibo.dip.pipeline.processor.Processor;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * stage pipeline实现，将各procesoor串行执行.
 * Create by hongxun on 2018/06/28
 */
public class PipelineStage extends Stage<Map<String, Object>> {

  private static final String engine = "*";

  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineStage.class);

  private List<Processor> processorList;

  private String metricsMeterName;
  private String metricsMeterResultName;

  public PipelineStage(List<Map<String, Object>> processorsCofnigList, String stageId) {
    super(stageId);
    processorList = createProcessorList(processorsCofnigList);
    metricsMeterName = String.format("%s_meter", stageId);
    metricsMeterResultName = String.format("%s_result_counter", stageId);
  }


  /**
   * 串行处理stage
   * 只有过滤的才会返回null，处理错误都抛异常
   *
   * @param data 处理数据
   */
  @Override
  public Map<String, Object> processStage(Map<String, Object> data) throws Exception {
    if (data == null) {
      return null;
    }
    MetricsSystem.getMeter(metricsMeterName).mark();
    try {
      for (Processor processor : processorList) {
        data = (Map<String, Object>) processor.process(data);
        if (data == null) {
          break;
        }
      }
    } catch (Exception e) {
      throw e;
    }
    if (data != null) {
      MetricsSystem.getCounter(metricsMeterResultName).inc();
    }
    return data;
  }

  /**
   * 创建processors.
   *
   * @param processorsCofnigList processors配置
   * @return 生成List
   */
  private List<Processor> createProcessorList(List<Map<String, Object>> processorsCofnigList) {
    List<Processor> processorList = Lists.newArrayList();

    for (Map<String, Object> params : processorsCofnigList) {
      String processorType = (String) params.get("processorType");
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      LOGGER.info(String
          .format("%s create processor: %s %s", stageId, processorType, subParams.toString()));

      Processor p = Processor.createProcessor(engine, processorType, subParams);

      if (p == null) {
        throw new RuntimeException(
            String.format("Processor type %s not exist!!!", processorType));
      }
      processorList.add(p);
    }

    return processorList;
  }
}
