package com.weibo.dip.pipeline.spark;

import com.codahale.metrics.MetricRegistry;
import com.weibo.dip.pipeline.extract.DatasetExactorTypeEnum;
import com.weibo.dip.pipeline.extract.DatasetExtractor;
import com.weibo.dip.pipeline.runner.Runner;
import com.weibo.dip.pipeline.stage.DatasetAggregateStage;
import com.weibo.dip.pipeline.stage.DatasetProcessStage;
import com.weibo.dip.pipeline.stage.Stage;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * 中间过程为dataset的runner父类，含一些公用方法
 * Create by hongxun on 2018/7/24
 */
public abstract class DatasetRunner extends Runner {

  protected SparkSession sparkSession;
  //  spark执行类型
  protected String engineType;
  protected String sourceFormat;
  protected Map<String, String> sourceOptions;
  protected List<Map<String, Object>> tables;

  protected Map<String, Object> preConfig;
  protected Map<String, Object> aggConfig;
  protected Map<String, Object> proConfig;

  protected String sinkFormat;
  protected String sinkMode;
  protected Map<String, String> sinkOptions;
  //  抽取器
  protected DatasetExtractor extractor;

  public DatasetRunner(Map<String, Object> configs) {
    //source配置
    engineType = (String) configs.get("engineType");
    Map<String, Object> sourceConfig = (Map<String, Object>) configs.get("sourceConfig");
    sourceFormat = (String) sourceConfig.get("format");
    sourceOptions = (Map<String, String>) sourceConfig.get("options");
    tables = (List<Map<String, Object>>) sourceConfig.get("tables");
    Map<String, Object> extractConfig = (Map<String, Object>) sourceConfig.get("extractor");
    extractor = DatasetExactorTypeEnum.getType(extractConfig);

    //process配置
    Map<String, Object> processConfig = (Map<String, Object>) configs.get("processConfig");
    preConfig = (Map<String, Object>) processConfig.get("pre");
    aggConfig = (Map<String, Object>) processConfig.get("agg");
    proConfig = (Map<String, Object>) processConfig.get("pro");
    //sink配置
    Map<String, Object> sinkConfig = (Map<String, Object>) configs.get("sinkConfig");
    sinkFormat = (String) sinkConfig.get("format");
    sinkMode = (String) sinkConfig.get("mode");
    sinkOptions = (Map<String, String>) sinkConfig.get("options");
  }

  /**
   * 抽取数据
   * @param dataset 数据集
   * @return 抽取结果数据集
   */
  protected Dataset extract(Dataset dataset) {
    if (extractor == null) {
      return dataset;
    }
    if ("kafka".equals(sourceFormat)) {
      dataset = dataset.selectExpr("CAST(value AS STRING) as _value_");
    }
    return extractor.extract(dataset);
  }

  /**
   *
   * @param dataset
   * @return
   * @throws Exception
   */
  protected Dataset process(Dataset dataset) throws Exception {
    if (preConfig != null) {
      dataset = pre(dataset);
    }
    if (aggConfig != null) {
      dataset = agg(dataset);
    }
    if (proConfig != null) {
      dataset = pro(dataset);
    }
    return dataset;
  }

  /**
   * 前处理阶段，在StructuredRunner只取第一个stage，因为没有条件所以全部可以在一个stage里
   *
   * @param dataset 数据集
   * @return 处理后数据集
   */
  @SuppressWarnings({"unchecked"})
  private Dataset pre(Dataset dataset) throws Exception {
    List<Map<String, Object>> stagesConfigList = (List<Map<String, Object>>) preConfig
        .get("stages");
    if (stagesConfigList != null && !stagesConfigList.isEmpty()) {
      return processStage(dataset, stagesConfigList);
    }
    return dataset;
  }

  /**
   * 执行聚合sql语句
   *
   * @param dataset 数据集
   * @return 数据集
   */
  private Dataset agg(Dataset dataset) throws Exception {
    DatasetAggregateStage aggregateStage = new DatasetAggregateStage(new MetricRegistry(),
        aggConfig, Stage.createStageId("DatasetAggregateStage"));
    return aggregateStage.processStage(dataset);
  }

  /**
   * 执行 - 后处理
   *
   * @param dataset 数据集
   * @return 数据集
   */
  @SuppressWarnings({"unchecked"})
  private Dataset pro(Dataset dataset) throws Exception {
    List<Map<String, Object>> stagesConfigList = (List<Map<String, Object>>) proConfig
        .get("stages");
    if (stagesConfigList != null && !stagesConfigList.isEmpty()) {
      return processStage(dataset, stagesConfigList);
    }
    return dataset;
  }

  /**
   * 处理pre及pro共用方法
   *
   * @param dataset 数据集
   * @param stagesConfigList stage配置
   * @return 数据集
   * @throws Exception 异常信息
   */
  @SuppressWarnings({"unchecked"})
  private Dataset processStage(Dataset dataset, List<Map<String, Object>> stagesConfigList)
      throws Exception {
    Map<String, Object> stageConfigMap = stagesConfigList.get(0);
    List<Map<String, Object>> processorConfigList = (List<Map<String, Object>>) stageConfigMap
        .get("processors");
    DatasetProcessStage processStage = new DatasetProcessStage(new MetricRegistry(),
        processorConfigList, Stage.createStageId("DatasetProcessStage"));
    return processStage.processStage(dataset);
  }


}
