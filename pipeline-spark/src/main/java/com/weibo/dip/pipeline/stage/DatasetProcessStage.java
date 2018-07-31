package com.weibo.dip.pipeline.stage;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.weibo.dip.pipeline.processor.DatasetProcessor;
import com.weibo.dip.pipeline.processor.DatasetProcessorTypeEnum;
import com.weibo.dip.pipeline.processor.Processor;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create by hongxun on 2018/7/16
 */
public class DatasetProcessStage extends Stage<Dataset> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetProcessStage.class);
  private List<DatasetProcessor> processorList;

  public DatasetProcessStage(List<Map<String, Object>> processorsCofnigList, String stageId) {
    super(stageId);
    processorList = createProcessorList(processorsCofnigList);
  }

  public Dataset processStage(Dataset dataset) throws Exception {

    for (DatasetProcessor processor : processorList) {
      dataset = processor.process(dataset);
    }
    return dataset;
  }

  /**
   * 创建processors.
   *
   * @param processorsCofnigList processors配置
   * @return 生成List
   */
  private List<DatasetProcessor> createProcessorList(
      List<Map<String, Object>> processorsCofnigList) {
    List<DatasetProcessor> processorList = Lists.newArrayList();
    for (Map<String, Object> params : processorsCofnigList) {
      String processorType = (String) params.get("processorType");
      String subType = Strings.nullToEmpty((String) params.get("subType"));
      LOGGER.info(String.format("%s create processor: %s(%s)", stageId, processorType, subType));

      DatasetProcessor p = DatasetProcessorTypeEnum.getType(processorType)
          .getProcessor(params);
      if (p == null) {
        throw new RuntimeException(String.format("Processor type %s not exist!!!", processorType));
      }
      processorList.add(p);

    }
    return processorList;
  }


}
