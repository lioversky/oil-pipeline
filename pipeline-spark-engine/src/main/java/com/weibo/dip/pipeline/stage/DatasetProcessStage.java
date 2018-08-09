package com.weibo.dip.pipeline.stage;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.weibo.dip.pipeline.processor.DatasetProcessor;
import com.weibo.dip.pipeline.processor.Processor;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * dataset的处理stage，对dataset的处理全部在一个stage中
 * Create by hongxun on 2018/7/16
 */
public class DatasetProcessStage extends Stage<Dataset> {

  private static String engine = "dataset";

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetProcessStage.class);
  /**
   * 处理器List，对dataset循环处理
   */
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
      LOGGER.info(String.format("%s create processor: %s", stageId, processorType));
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      DatasetProcessor p = (DatasetProcessor) Processor
          .createProcessor(engine, processorType, subParams);

      if (p == null) {
        throw new RuntimeException(
            String.format("Processor type %s not exist!!!", processorType));
      }
      processorList.add(p);

    }

    return processorList;
  }


}
