package com.weibo.dip.pipeline.stage;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.weibo.dip.pipeline.processor.Processor;
import com.weibo.dip.pipeline.processor.ProcessorTypeEnum;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * stage pipeline实现，将各procesoor串行执行.
 * Create by hongxun on 2018/06/28
 */
public class PipelineStage extends Stage<Map<String, Object>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineStage.class);

  private List<Processor> processorList;
  private Timer stageTimer;

  public PipelineStage(MetricRegistry metricRegistry,
      List<Map<String, Object>> processorsCofnigList, String stageId) {
    super(metricRegistry, stageId);
    processorList = createProcessorList(processorsCofnigList);
    stageTimer = metricRegistry.timer(String.format("%s_timer", stageId));

  }


  /**
   * 串行处理stage
   */
  @Override
  public Map<String, Object> processStage(Map<String, Object> data) throws Exception {
    if (data == null) {
      return null;
    }
    Context context = stageTimer.time();
    try {
      for (Processor processor : processorList) {
        data = (Map<String, Object>) processor.process(data);
        if (data == null) {
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
      String subType = Strings.nullToEmpty((String) params.get("subType"));
      LOGGER.info(String.format("%s create processor: %s(%s)", stageId, processorType, subType));
      Processor p = ProcessorTypeEnum.getType(processorType)
          .getProcessor(params);
      if (p == null) {
        throw new RuntimeException(String.format("Processor type %s not exist!!!", processorType));
      }
      processorList.add(p);

    }
    return processorList;
  }
}
