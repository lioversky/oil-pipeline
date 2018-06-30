package com.weibo.dip.pipeline.stage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.collect.Lists;
import com.weibo.dip.pipeline.processor.Processor;
import com.weibo.dip.pipeline.processor.ProcessorTypeEnum;
import java.util.List;
import java.util.Map;

/**
 * stage pipeline实现，将各procesoor串行执行.
 * Create by hongxun on 2018/06/28
 */
public class PipelineStage extends Stage {

  private List<Processor> processorList;
  private Timer stageTimer;

  public PipelineStage(List<Map<String, Object>> processorsCofnigList, String stageId) {
    super(new MetricRegistry(), stageId);
    processorList = createProcessorList(processorsCofnigList);
    stageTimer = metricRegistry.timer(String.format("PipelineStage_%d_timer", stageId));
  }


  /**
   * 串行处理stage
   * @param data
   * @return
   * @throws Exception
   */
  @Override
  public Map<String, Object> processStage(Map<String, Object> data) throws Exception {
    if (data == null) {
      return null;
    }
    Context context = stageTimer.time();
    try {
      for (Processor processor : processorList) {
        data = processor.process(data);
        if (data == null) {
          break;
        }
      }
      if (data != null) {
        //todo:
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
